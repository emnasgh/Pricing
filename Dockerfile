FROM apache/airflow:2.8.0-python3.10
USER root
RUN apt-get update && apt-get install -y \
    g++ \
    build-essential
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt