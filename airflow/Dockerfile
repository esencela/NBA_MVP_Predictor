FROM apache/airflow:2.8.1-python3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgomp1 \
        g++ \
        build-essential \
        wget \
        curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /requirements.txt

USER airflow

# Upgrade pip
RUN python -m pip install --upgrade pip

# Install ML dependencies
RUN pip install --no-cache-dir -r /requirements.txt

