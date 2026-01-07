# Cloud Scheduler Jobs

## extract-daily
- Trigger: HTTP
- URL: Cloud Function EXTRACT
- Schedule: 0 9 * * *
- Timezone: Europe/Paris

## transform-daily
- Trigger: HTTP
- URL: Cloud Function LOAD
- Schedule: 15 9 * * *
- Timezone: Europe/Paris
