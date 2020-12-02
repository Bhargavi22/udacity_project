#!/usr/bin/env bash
docker run -d -p 8080:8080 -e LOAD_EX=n  -v $AIRFLOW_HOME/requirements.txt:/requirements.txt -v $AIRFLOW_HOME/dags:/usr/local/airflow/dags -v $AIRFLOW_HOME/plugins:/usr/local/airflow/plugins -v $AIRFLOW_HOME/tmp:/usr/local/airflow/tmp puckel/docker-airflow webserver