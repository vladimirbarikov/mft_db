FROM apache/airflow:2.9.3

COPY requirements.txt /tmp/requirements.txt

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gosu \
        gcc \
        python3-dev && \
    rm -rf /var/lib/apt/lists/*


USER airflow

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt && \
    pip cache purge
