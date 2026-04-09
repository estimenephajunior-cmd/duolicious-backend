#!/usr/bin/env bash

script_dir="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$script_dir"

set -euo pipefail

export PYTHONUNBUFFERED=true
export PYTHONDONTWRITEBYTECODE=true
export PYTHONPATH=.

if [[ -z "${PORT:-}" ]]; then
  PORT=5000
fi

python3 database/initapi_render.py

exec gunicorn \
  --workers "${DUO_WORKERS:-1}" \
  --bind "0.0.0.0:$PORT" \
  --timeout 0 \
  service.api:app
