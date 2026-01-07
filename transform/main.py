"""
Cloud Function 2 : transform - GCS → BigQuery
Trigger: HTTP (Scheduler)
Entry point: load_to_bigquery

- récupère le fichier RAW de target_date
- transforme + agrège journalier
- charge dans BigQuery (idempotent via delete date + append)
"""

import gzip
import io
import json
import os
from datetime import date, datetime, timedelta, timezone
from typing import Optional, Tuple

import functions_framework
import pandas as pd
from google.cloud import bigquery, storage

# -----------------------------
# ENV
# -----------------------------
PROJECT_ID = os.environ.get("PROJECT_ID", "").strip()
BQ_RUNS_TABLE = os.environ.get("BQ_RUNS_TABLE", "airq_ops.pipeline_runs").strip()
BQ_DATA_TABLE = os.environ.get("BQ_DATA_TABLE", "airq_data.air_quality_history").strip()
BUCKET_NAME = os.environ.get("BUCKET_NAME", "").strip()

SOURCE = "openmeteo_air"

_storage_client = None
_bq_client = None


def get_storage_client() -> storage.Client:
    global _storage_client
    if _storage_client is None:
        _storage_client = storage.Client()
    return _storage_client


def get_bq_client() -> bigquery.Client:
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=PROJECT_ID or None)
    return _bq_client


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def json_response(payload: dict, status: int = 200):
    return (json.dumps(payload), status, {"Content-Type": "application/json"})


def validate_env() -> list[str]:
    missing = []
    if not PROJECT_ID:
        missing.append("PROJECT_ID")
    if not BUCKET_NAME:
        missing.append("BUCKET_NAME")
    if not BQ_RUNS_TABLE:
        missing.append("BQ_RUNS_TABLE")
    if not BQ_DATA_TABLE:
        missing.append("BQ_DATA_TABLE")
    return missing


def runs_table_fqn() -> str:
    return f"{PROJECT_ID}.{BQ_RUNS_TABLE}"


def data_table_fqn() -> str:
    return f"{PROJECT_ID}.{BQ_DATA_TABLE}"


def log_run_insert(
    run_id: str,
    target_date: str,
    status: str,
    records_loaded: Optional[int] = None,
    gcs_raw_path: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:
    """Insert un log (simple)."""
    try:
        row = {
            "run_id": run_id,
            "target_date": target_date,
            "source": SOURCE,
            "status": status,
            "started_at": now_utc().isoformat() if status == "LOAD_STARTED" else now_utc().isoformat(),
            "ended_at": now_utc().isoformat() if status in ("LOAD_SUCCESS", "LOAD_FAILED") else None,
            "records_out": records_loaded,
            "gcs_raw_path": gcs_raw_path,
            "error_message": error_message,
        }
        errors = get_bq_client().insert_rows_json(runs_table_fqn(), [row])
        if errors:
            print({"level": "ERROR", "msg": "BQ log insert failed", "errors": errors})
    except Exception as e:
        print({"level": "ERROR", "msg": "Failed to log to BigQuery", "err": str(e)})


def parse_partitioned_run_id(blob_name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    raw/source=openmeteo_air/date=YYYY-MM-DD/run_id=.../data.jsonl.gz
    """
    run_id = None
    date_found = None
    for p in blob_name.split("/"):
        if p.startswith("date="):
            date_found = p.replace("date=", "")
        if p.startswith("run_id="):
            run_id = p.replace("run_id=", "")
    return run_id, date_found


def list_candidates_for_date(target_date: str):
    """
    On liste TOUS les fichiers jsonl.gz pour la date.
    On choisit le plus récent.
    """
    bucket = get_storage_client().bucket(BUCKET_NAME)

    prefixes = [
        f"raw/{target_date}/",  # raw/YYYY-MM-DD/<run_id>.jsonl.gz
        f"raw/source={SOURCE}/date={target_date}/",  # raw/source=.../date=.../run_id=.../data.jsonl.gz
        f"raw/source=openmeteo_air/date={target_date}/",
    ]

    candidates = []
    for prefix in prefixes:
        for b in bucket.list_blobs(prefix=prefix):
            if b.name.endswith(".jsonl.gz"):
                candidates.append(b)

    candidates.sort(key=lambda b: b.time_created, reverse=True)
    return candidates


def pick_raw_blob_for_date(target_date: str):
    candidates = list_candidates_for_date(target_date)
    if not candidates:
        return None
    return candidates[0]


def extract_run_id_and_date(blob_name: str, fallback_date: str) -> Tuple[str, str]:
    if "run_id=" in blob_name:
        run_id, date_found = parse_partitioned_run_id(blob_name)
        return (run_id or "unknown"), (date_found or fallback_date)

    # raw/YYYY-MM-DD/<run_id>.jsonl.gz
    parts = blob_name.split("/")
    if len(parts) >= 3:
        date_found = parts[1] or fallback_date
        run_id = parts[2].replace(".jsonl.gz", "")
        return run_id, date_found

    return "unknown", fallback_date


def load_gz_jsonl_to_df(content: bytes) -> pd.DataFrame:
    with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
        text = gz.read().decode("utf-8").strip()
    if not text:
        return pd.DataFrame()
    return pd.DataFrame([json.loads(line) for line in text.split("\n") if line.strip()])


def transform_daily(df: pd.DataFrame) -> pd.DataFrame:
    required = {"time", "city", "country"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in raw data: {sorted(list(missing))}")

    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df["date"] = df["time"].dt.date

    # Agrégations
    agg = {}
    for col in ["pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide", "ozone", "european_aqi"]:
        if col in df.columns:
            agg[col] = "mean"

    # Colonnes fixes
    if "population" in df.columns:
        agg["population"] = "first"
    if "latitude" in df.columns:
        agg["latitude"] = "first"
    if "longitude" in df.columns:
        agg["longitude"] = "first"

    if not agg:
        raise ValueError("No numeric columns found to aggregate")

    daily = (
        df.groupby(["city", "country", "date"])
        .agg(agg)
        .reset_index()
    )

    daily["date"] = pd.to_datetime(daily["date"]).dt.date
    return daily


def delete_existing_partition(target_date: str):
    """
    Idempotence simple :
    on supprime les lignes existantes de ce jour avant de réinsérer.
    (Très pro pour backfill + relance)
    """
    q = f"DELETE FROM `{data_table_fqn()}` WHERE date = @d"
    job = get_bq_client().query(
        q,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("d", "DATE", target_date)]
        ),
    )
    job.result()


def load_df_to_bigquery(df: pd.DataFrame):
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    job = get_bq_client().load_table_from_dataframe(df, data_table_fqn(), job_config=job_config)
    job.result()


@functions_framework.http
def load_to_bigquery(request):
    missing = validate_env()
    if missing:
        return json_response({"status": "CONFIG_ERROR", "missing_env_vars": missing}, 500)

    body = request.get_json(silent=True) or {}
    target_date = body.get("target_date") or (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    # Choisir le fichier RAW STRICTEMENT pour cette date
    blob = pick_raw_blob_for_date(target_date)
    if not blob:
        return json_response(
            {
                "status": "NO_FILE",
                "target_date": target_date,
                "bucket": BUCKET_NAME,
                "hint": "Vérifie qu'un raw/<date>/...jsonl.gz existe pour cette date",
            },
            404,
        )

    run_id, date_found = extract_run_id_and_date(blob.name, target_date)
    gcs_uri = f"gs://{BUCKET_NAME}/{blob.name}"

    print(f"[{run_id}] LOAD start date={date_found} file={gcs_uri}")
    log_run_insert(run_id, date_found, "LOAD_STARTED", gcs_raw_path=gcs_uri)

    try:
        content = blob.download_as_bytes()
        df_raw = load_gz_jsonl_to_df(content)
        if df_raw.empty:
            log_run_insert(run_id, date_found, "LOAD_FAILED", gcs_raw_path=gcs_uri, error_message="EMPTY_RAW_FILE")
            return json_response({"status": "EMPTY_RAW_FILE", "run_id": run_id}, 400)

        df_daily = transform_daily(df_raw)

        # IMPORTANT: idempotence (évite doublons si relance)
        delete_existing_partition(date_found)
        load_df_to_bigquery(df_daily)

        log_run_insert(run_id, date_found, "LOAD_SUCCESS", records_loaded=int(len(df_daily)), gcs_raw_path=gcs_uri)

        return json_response(
            {
                "status": "LOAD_SUCCESS",
                "run_id": run_id,
                "target_date": date_found,
                "gcs_raw_path": gcs_uri,
                "records_raw": int(len(df_raw)),
                "records_loaded": int(len(df_daily)),
                "table": data_table_fqn(),
                "distinct_cities_loaded": int(df_daily["city"].nunique()),
            },
            200,
        )

    except Exception as e:
        err = str(e)[:1000]
        print(f"[{run_id}] LOAD failed: {err}")
        log_run_insert(run_id, date_found, "LOAD_FAILED", gcs_raw_path=gcs_uri, error_message=err)
        return json_response({"status": "LOAD_FAILED", "run_id": run_id, "target_date": date_found, "error": err}, 500)
