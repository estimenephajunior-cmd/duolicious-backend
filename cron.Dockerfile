# syntax=docker.io/docker/dockerfile:1.7-labs
FROM python:3.12-slim

ENV DUO_USE_VENV=false
ENV PYTHONUNBUFFERED=true

WORKDIR /app

COPY \
  --exclude=antiabuse/antiporn/model.onnx.part00 \
  --exclude=antiabuse/antiporn/model.onnx.part01 \
  --exclude=antiabuse/antiporn/model.onnx.part02 \
  --exclude=antiabuse/antiporn/model.onnx.part03 \
  --exclude=antiabuse/antiporn/model.onnx.part04 \
  --exclude=test \
  --exclude=vm \
  . /app

RUN : \
  && pip install --no-cache-dir -r /app/requirements.txt \
  && python - <<'PY'
from pathlib import Path
from urllib.request import urlretrieve

base = "https://raw.githubusercontent.com/estimenephajunior-cmd/duolicious-backend/main/antiabuse/antiporn"
target = Path("/app/antiabuse/antiporn")
target.mkdir(parents=True, exist_ok=True)

for name in [
    "model.onnx.part00",
    "model.onnx.part01",
    "model.onnx.part02",
    "model.onnx.part03",
    "model.onnx.part04",
]:
    urlretrieve(f"{base}/{name}", target / name)
PY

CMD ["bash", "/app/cron.main.sh"]
