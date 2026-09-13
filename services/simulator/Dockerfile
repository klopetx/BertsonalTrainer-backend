FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /opt/app

COPY requirements/simulator.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY services/simulator/bertsonal_simulator ./bertsonal_simulator

ENTRYPOINT ["python", "-m", "bertsonal_simulator.cli"]
