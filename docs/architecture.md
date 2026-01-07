# Architecture du projet (Vue globale)

## Objectif
Collecter des données de qualité de l’air dans les villes européennes, historiser les données brutes, agréger les indicateurs au jour, et exposer un dashboard citoyen / analytique.

## Composants
- **Cloud Scheduler** : déclenche les jobs quotidiens
- **Cloud Function EXTRACT** : collecte API Open-Meteo et écrit les fichiers bruts en GCS
- **Cloud Storage (GCS)** : data lake (zone RAW)
- **Cloud Function TRANSFORM/LOAD** : lit RAW, transforme et charge en BigQuery
- **BigQuery** : data warehouse (tables PROD) + table d’historique d’exécution
- **Looker Studio** : visualisation (cartes + séries temporelles)

## Stockage
- **RAW** (GCS) : format JSONL compressé `.jsonl.gz`
- **PROD** (BigQuery) : agrégats journaliers par ville

## Observabilité
- Table `airq_ops.pipeline_runs` : statut, date, nombre de lignes, chemin GCS, erreurs.
- Cloud Logging : logs techniques + debugging.

## Sécurité (résumé)
- Service Account dédié ETL
- Permissions minimales sur Bucket et BigQuery
