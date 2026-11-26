# Standalone OCI worker image for OCR / ML eval / agent jobs (Cloud Run Job / GKE).
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

RUN apt-get update \
 && apt-get install -y --no-install-recommends tesseract-ocr tesseract-ocr-eng \
 && rm -rf /var/lib/apt/lists/*

COPY requirements-worker.txt /tmp/requirements-worker.txt
RUN pip install -r /tmp/requirements-worker.txt \
 && pip install "https://github.com/explosion/spacy-models/releases/download/xx_ent_wiki_sm-3.7.0/xx_ent_wiki_sm-3.7.0-py3-none-any.whl"
# The worker image can also run dbt as a one-shot job (CNCF portability path).
COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt

WORKDIR /app
COPY common ./common
COPY ingestion ./ingestion
COPY ml ./ml
COPY agent ./agent
COPY observability ./observability
COPY scripts ./scripts
COPY dbt_pii_hygiene ./dbt_pii_hygiene

ENTRYPOINT ["python", "-m"]
