FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && java_bin="$(readlink -f "$(command -v java)")" \
    && ln -sfn "$(dirname "$(dirname "$java_bin")")" /usr/lib/jvm/default-java \
    && printf '%s\n' \
        '#!/bin/sh' \
        'mkdir -p /app/data/raw/sensor /app/data/raw/riego /app/data/raw/clima /app/data/curated /app/data/checkpoints' \
        'exec "$@"' \
        > /usr/local/bin/entrypoint.sh \
    && chmod +x /usr/local/bin/entrypoint.sh

ENV JAVA_HOME=/usr/lib/jvm/default-java \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    SPARK_DRIVER_MEMORY=768m

WORKDIR /app

COPY riego_bigdata/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY riego_bigdata/ .

ENTRYPOINT ["entrypoint.sh"]
CMD ["python", "-c", "import config; print('imagen riego lista', config.DATA_DIR)"]
