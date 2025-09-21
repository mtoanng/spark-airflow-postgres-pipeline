#!/bin/bash
echo "Stopping Airflow..."
pkill -f "airflow webserver"
pkill -f "airflow scheduler"
echo "Airflow stopped."
