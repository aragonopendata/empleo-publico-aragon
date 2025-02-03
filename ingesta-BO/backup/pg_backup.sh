#!/bin/bash

# credenciales
source /app/.env
pg_dump --dbname=postgresql://$DB_USR:$DB_PASS@$DB_HOST:$DB_PORT/$DB_NAME > backup.sql
