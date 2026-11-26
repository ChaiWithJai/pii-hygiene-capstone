# Airflow image with the project's Python runtime + OCR binary.
# Deterministic: base image tag and pip constraints are pinned.
FROM apache/airflow:2.10.5-python3.12

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends tesseract-ocr tesseract-ocr-eng \
 && rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements-worker.txt requirements-dbt.txt /tmp/
RUN pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt" \
      -r /tmp/requirements-worker.txt \
 && pip install --no-cache-dir \
      "https://github.com/explosion/spacy-models/releases/download/xx_ent_wiki_sm-3.7.0/xx_ent_wiki_sm-3.7.0-py3-none-any.whl" \
 && python3 -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt

ENV PYTHONPATH=/opt/pii \
    DBT_BINARY=/home/airflow/dbt-venv/bin/dbt
