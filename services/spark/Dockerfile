FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless curl procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

COPY requirements/spark.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /opt/app
COPY services/spark/bertsonal_spark ./bertsonal_spark

ENV PYTHONPATH=/opt/app

ENTRYPOINT ["python", "-m", "bertsonal_spark.streaming.main"]
