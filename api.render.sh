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

bootstrap_server_pid=""

start_bootstrap_server() {
  python3 - <<'PY' &
import http.server
import json
import os
import socketserver

PORT = int(os.environ["PORT"])


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            status = 200
            body = {
                "status": "starting",
                "message": "Render backend is initializing",
            }
        else:
            status = 503
            body = {
                "status": "starting",
                "message": "Render backend is initializing",
            }

        payload = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format, *args):
        return


class ReusableTCPServer(socketserver.TCPServer):
    allow_reuse_address = True


with ReusableTCPServer(("0.0.0.0", PORT), Handler) as httpd:
    httpd.serve_forever()
PY

  bootstrap_server_pid=$!
}

stop_bootstrap_server() {
  if [[ -n "${bootstrap_server_pid}" ]]; then
    kill "${bootstrap_server_pid}" >/dev/null 2>&1 || true
    wait "${bootstrap_server_pid}" 2>/dev/null || true
    bootstrap_server_pid=""
  fi
}

trap stop_bootstrap_server EXIT

start_bootstrap_server

python3 database/initapi_render.py

stop_bootstrap_server

exec gunicorn \
  --workers "${DUO_WORKERS:-1}" \
  --bind "0.0.0.0:$PORT" \
  --timeout 0 \
  service.api:app
