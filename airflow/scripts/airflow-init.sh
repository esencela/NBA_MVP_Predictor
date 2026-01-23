#!/user/bin/env bash
set -e

echo "Starting Airflow bootstrap"

echo "Running Airflow migrations..."
airflow db migrate

echo "Creating Airflow pools..."
airflow pools set api_pool 1 "Extraction request limit"

echo "Airflow bootstrap complete"