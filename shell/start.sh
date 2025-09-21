#!/bin/bash
VENV_PATH=~/airflow_venv

source $VENV_PATH/bin/activate

export AIRFLOW_HOME=~/airflow
LOG_DIR=$AIRFLOW_HOME/logs

mkdir -p $LOG_DIR

echo "Starting Airflow Webserver..."
airflow webserver -p 8080 > $LOG_DIR/webserver.log 2>&1 &

echo "Starting Airflow Scheduler..."
airflow scheduler > $LOG_DIR/scheduler.log 2>&1 &

echo "Airflow is running in background."
echo "Web UI: http://localhost:8080"

