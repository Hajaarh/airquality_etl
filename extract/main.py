"""
Cloud Function 1 : EXTRACT
- GeoNames depuis GCS: cities15000.zip (contient >15k)
- Filtre: Europe + population >= 100000 (garanti)
- Appel Open-Meteo Air Quality
- Output: JSONL.GZ dans GCS (avec lat/lon/pop)
Entry point: extract_to_gcs
"""

import gzip
import io
import json
import os
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional

import functions_framework
import pandas as pd
import requests
from google.cloud import bigquery, storage

# =============================================================================
# ENV
# =============================================================================
PROJECT_ID = os.environ.get("PROJECT_ID", "").strip()
BUCKET_NAME = os.environ.get("BUCKET_NAME", "").strip()
BQ_RUNS_TABLE = os.environ.get("BQ_RUNS_TABLE", "").strip()

GEONAMES_ZIP_PATH = os.environ.get("GEONAMES_ZIP_PATH", "cities15000.zip").strip()

# ✅ Default 100k
MIN_POPULATION = int(os.environ.get("MIN_POPULATION", "100000"))
# Garde-fou: on refuse en dessous de 100k (pour éviter raw_pop=25)
MIN_ALLOWED_POP = int(os.environ.get("MIN_ALLOWED_POP", "100000"))

MAX_CITIES = int(os.environ.get("MAX_CITIES", "0"))  # 0 = illimité
THREADS = int(os.environ.get("THREADS", "30"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "20"))

SOURCE = "openmeteo_air"
BASE_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

# ✅ Liste Europe (ISO country codes)
EUROPE = [
    "AL","AT","BA","BE","BG","CH","CZ","DE","DK","EE","ES","FI","FR","GB","GR",
    "HR","HU","IE","IS","IT","LT","LU","LV","MD","ME","MK","MT","NL","NO","PL",
    "PT","RO","RS","SE","SI","SK","UA"
]

storage_client = storage.Client()
bq_client = bigquery.Client(project=PROJECT_ID or None)

# =============================================================================
# HELPERS
# =============================================================================
def now_utc():
    return datetime.now(timezone.utc)

def json_response(payload: dict, status: int = 200):
    return (json.dumps(payload), status, {"Content-Type": "application/json"})

def get_target_date(request):
    if request.args and request.args.get("target_date"):
        return request.args["target_date"]
    body = request.get_json(silent=True)
    if body and body.get("target_date"):
        return body["target_date"]
    return (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

def get_run_id(request):
    body = request.get_json(silent=True)
    if body and body.get("run_id"):
        return body["run_id"]
    return str(uuid.uuid4())

def log_run(run_id, target_date, status, records_out=None, gcs_path=None, error_message=None):
    if not (PROJECT_ID and BQ_RUNS_TABLE):
        return
    table_id = f"{PROJECT_ID}.{BQ_RUNS_TABLE}"
    rows = [{
        "run_id": run_id,
        "target_date": target_date,
        "source": SOURCE,
        "status": status,
        "started_at": now_utc().isoformat() if status == "EXTRACT_STARTED" else None,
        "ended_at": now_utc().isoformat() if status in ("EXTRACT_SUCCESS","EXTRACT_FAILED") else None,
        "records_out": records_out,
        "gcs_raw_path": gcs_path,
        "error_message": error_message,
    }]
    bq_client.insert_rows_json(table_id, rows)

def validate_config():
    if not PROJECT_ID:
        return False, "Missing PROJECT_ID"
    if not BUCKET_NAME:
        return False, "Missing BUCKET_NAME"
    if MIN_POPULATION < MIN_ALLOWED_POP:
        return False, f"MIN_POPULATION too low ({MIN_POPULATION}). Must be >= {MIN_ALLOWED_POP}."
    if THREADS < 1 or THREADS > 80:
        return False, "THREADS must be between 1 and 80"
    return True, None

# =============================================================================
# LOAD CITIES FROM GCS ZIP
# =============================================================================
def load_geonames_df() -> pd.DataFrame:
    blob = storage_client.bucket(BUCKET_NAME).blob(GEONAMES_ZIP_PATH)
    if not blob.exists():
        raise RuntimeError(f"GeoNames ZIP not found: gs://{BUCKET_NAME}/{GEONAMES_ZIP_PATH}")

    with zipfile.ZipFile(io.BytesIO(blob.download_as_bytes()), "r") as z:
        filename = z.namelist()[0]
        with z.open(filename) as f:
            df = pd.read_csv(f, sep="\t", header=None, low_memory=False)

    df.columns = [
        "geonameid","name","asciiname","alternatenames","latitude","longitude",
        "feature_class","feature_code","country_code","cc2","admin1_code",
        "admin2_code","admin3_code","admin4_code","population","elevation",
        "dem","timezone","modification_date"
    ]
    return df

def build_city_list() -> List[Dict]:
    df = load_geonames_df()

    df = df[df["country_code"].isin(EUROPE)]
    df = df[df["population"] >= MIN_POPULATION]

    if df.empty:
        raise RuntimeError(f"No cities after filtering Europe + pop>={MIN_POPULATION}")

    cities = (
        df[["name","country_code","latitude","longitude","population"]]
        .rename(columns={"name":"city","country_code":"country"})
        .drop_duplicates(subset=["city","country","latitude","longitude"])
        .sort_values(["country", "population"], ascending=[True, False])
    )

    if MAX_CITIES > 0:
        cities = cities.head(MAX_CITIES)

    return cities.to_dict(orient="records")

# =============================================================================
# API CALL
# =============================================================================
def fetch_city(session: requests.Session, city: Dict, target_date: str) -> Optional[pd.DataFrame]:
    try:
        r = session.get(
            BASE_URL,
            params={
                "latitude": city["latitude"],
                "longitude": city["longitude"],
                "hourly": "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone,european_aqi",
                "start_date": target_date,
                "end_date": target_date,
                "timezone": "auto",
            },
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()

        hourly = data.get("hourly")
        if not hourly:
            return None

        df = pd.DataFrame(hourly)
        if df.empty:
            return None

        df["city"] = city["city"]
        df["country"] = city["country"]
        df["population"] = city["population"]
        df["latitude"] = city["latitude"]
        df["longitude"] = city["longitude"]
        return df
    except Exception:
        return None

# =============================================================================
# MAIN
# =============================================================================
@functions_framework.http
def extract_to_gcs(request):
    ok, err = validate_config()
    if not ok:
        return json_response({"status": "CONFIG_ERROR", "error": err}, 500)

    run_id = get_run_id(request)
    target_date = get_target_date(request)

    log_run(run_id, target_date, "EXTRACT_STARTED")

    try:
        cities = build_city_list()
        cities_total = len(cities)
        min_pop_selected = min(c["population"] for c in cities)
        max_pop_selected = max(c["population"] for c in cities)

        session = requests.Session()
        frames: List[pd.DataFrame] = []

        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            futures = [executor.submit(fetch_city, session, c, target_date) for c in cities]
            for i, f in enumerate(as_completed(futures), 1):
                df = f.result()
                if df is not None:
                    frames.append(df)
                if i % 200 == 0:
                    print(f"[{run_id}] processed {i}/{cities_total} cities...")

        if not frames:
            log_run(run_id, target_date, "EXTRACT_FAILED", error_message="NO_DATA")
            return json_response(
                {"status": "NO_DATA", "run_id": run_id, "date": target_date, "cities_total": cities_total},
                200,
            )

        df_all = pd.concat(frames, ignore_index=True)

        # ✅ Chemin explicite: raw_pop=100000/...
        gcs_path = f"raw/{target_date}/{run_id}.jsonl.gz"
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            for row in df_all.to_dict(orient="records"):
                gz.write((json.dumps(row, default=str) + "\n").encode("utf-8"))

        storage_client.bucket(BUCKET_NAME).blob(gcs_path).upload_from_string(
            buf.getvalue(), content_type="application/gzip"
        )

        gcs_uri = f"gs://{BUCKET_NAME}/{gcs_path}"

        log_run(run_id, target_date, "EXTRACT_SUCCESS", int(len(df_all)), gcs_uri)

        return json_response(
            {
                "status": "EXTRACT_SUCCESS",
                "run_id": run_id,
                "date": target_date,
                "min_population_config": MIN_POPULATION,
                "min_population_selected": int(min_pop_selected),
                "max_population_selected": int(max_pop_selected),
                "cities_total": int(cities_total),
                "cities_with_data": int(len(frames)),
                "records": int(len(df_all)),
                "gcs_path": gcs_uri,
            },
            200,
        )

    except Exception as e:
        msg = str(e)[:1000]
        log_run(run_id, target_date, "EXTRACT_FAILED", error_message=msg)
        return json_response({"status": "EXTRACT_FAILED", "run_id": run_id, "date": target_date, "error": msg}, 500)
