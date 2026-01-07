# Dictionnaire de données

## 1) Zone RAW (GCS)

### Emplacement
`gs://<BUCKET_NAME>/raw/<YYYY-MM-DD>/<run_id>.jsonl.gz`

### Format
- **JSON Lines** (1 ligne = 1 mesure horaire pour une ville)
- compressé en **gzip**

### Schéma RAW (par ligne)
| Champ | Type | Nullable | Exemple | Description |
|------|------|----------|---------|-------------|
| time | STRING (ISO) | Non | 2026-01-06T23:00 | Timestamp horaire local retourné par l’API |
| pm10 | FLOAT | Oui | 17.8 | Particules PM10 (µg/m³) |
| pm2_5 | FLOAT | Oui | 14.1 | Particules PM2.5 (µg/m³) |
| carbon_monoxide | FLOAT | Oui | 265.0 | CO (µg/m³ ou mg/m³ selon source) |
| nitrogen_dioxide | FLOAT | Oui | 20.8 | NO₂ |
| sulphur_dioxide | FLOAT | Oui | 1.5 | SO₂ |
| ozone | FLOAT | Oui | 30.0 | O₃ |
| european_aqi | FLOAT/INT | Oui | 26 | Indice AQI Europe |
| city | STRING | Non | Paris | Nom de la ville |
| country | STRING | Non | FR | Code pays (ISO2) |
| population | INT | Oui | 2148000 | Population GeoNames |
| latitude | FLOAT | Oui | 48.8566 | Latitude |
| longitude | FLOAT | Oui | 2.3522 | Longitude |

---

## 2) Table de suivi pipeline (BigQuery)

### Table
`airq_ops.pipeline_runs`

### Objectif
Historiser chaque exécution (traçabilité / monitoring / debugging).

| Champ | Type | Nullable | Description |
|------|------|----------|-------------|
| run_id | STRING | Non | Identifiant unique d’un run |
| target_date | DATE | Non | Date traitée |
| source | STRING | Non | openmeteo_air |
| status | STRING | Non | EXTRACT_STARTED / EXTRACT_SUCCESS / LOAD_SUCCESS / LOAD_FAILED… |
| started_at | TIMESTAMP | Oui | Heure de démarrage |
| ended_at | TIMESTAMP | Oui | Heure de fin |
| records_out | INT64 | Oui | Nombre de lignes produites (raw ou daily) |
| gcs_raw_path | STRING | Oui | URI du fichier RAW |
| error_message | STRING | Oui | Message d’erreur si échec |

---

## 3) Table PROD (BigQuery)

### Table
`airq_data.air_quality_history`

### Granularité
**1 ligne = 1 ville, 1 date (agrégation journalière)**

| Champ | Type | Nullable | Description |
|------|------|----------|-------------|
| city | STRING | Non | Ville |
| country | STRING | Non | Pays |
| date | DATE | Non | Date du jour |
| pm10 | FLOAT64 | Oui | Moyenne journalière |
| pm2_5 | FLOAT64 | Oui | Moyenne journalière |
| carbon_monoxide | FLOAT64 | Oui | Moyenne journalière |
| nitrogen_dioxide | FLOAT64 | Oui | Moyenne journalière |
| sulphur_dioxide | FLOAT64 | Oui | Moyenne journalière |
| ozone | FLOAT64 | Oui | Moyenne journalière |
| european_aqi | FLOAT64 | Oui | Moyenne journalière |
| population | INT64 | Oui | Population (valeur fixe ville) |
| latitude | FLOAT64 | Oui | Coordonnée ville |
| longitude | FLOAT64 | Oui | Coordonnée ville |

### Règles de transformation
- `date = DATE(time)`
- agrégation : **mean** sur les polluants, **first** sur population/lat/lon
- idempotence : suppression des lignes existantes pour la date avant insert
