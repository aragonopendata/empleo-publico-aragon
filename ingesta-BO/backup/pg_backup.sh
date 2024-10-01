#!/bin/bash

# credenciales
DB_NAME=empleo_publico_aragon
DB_HOST=172.27.38.82
DB_PORT=5432
DB_USR=empleo_publico
DB_PASS=KcpHS1V5YE

pg_dump --dbname=postgresql://$DB_USR:$DB_PASS@$DB_HOST:$DB_PORT/$DB_NAME > /opt/airflow/dags/backup.sql
