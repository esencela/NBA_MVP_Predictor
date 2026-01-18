# Use Airflow with Python 3.11
FROM apache/airflow:2.8.1-python3.11

# Switch to airflow to install dependencies
USER airflow

# Copy requirements into the image
COPY requirements.txt /requirements.txt

# Upgrade pip and install requirements
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt