#!/bin/bash

# credenciales
source /app/.env
pg_dump --dbname=postgresql://$DB_EMPLEO_USER:$DB_EMPLEO_PASS@$BACK_HOST:$DB_EMPLEO_PORT/$DB_EMPLEO_NAME > backup.sql
