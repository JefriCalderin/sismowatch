FROM apache/airflow:2.8.2-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt && \
    ln -s /opt/airflow/include /home/airflow/.local/lib/python3.11/site-packages/airflow/include
