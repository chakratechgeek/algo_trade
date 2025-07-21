#!/bin/bash
docker compose run --rm airflow-webserver airflow db init

docker compose run --rm airflow-webserver airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email chakraodba@gmail.com \
  --password qAzwsxedc@2187