#!/usr/bin/env bash
set -euo pipefail

APP_DIR=/home/admin/mintzy-plugin
cd "$APP_DIR"

venv/bin/python -m py_compile \
  auto_trader.py auto_trader_exposure_expansion.py auto_trader_exposure_expansion_org.py \
  api_server.py session_manager.py utils/*.py scripts/*.py

pkill -f 'gunicorn -w 2 -k uvicorn.workers.UvicornWorker' || true
screen -S mintzy-plugin -X quit || true
sleep 3

screen -dmS mintzy-plugin bash -c \
  "cd $APP_DIR && venv/bin/gunicorn -w 2 -k uvicorn.workers.UvicornWorker api_server:app --bind 0.0.0.0:8000 --access-logfile - --error-logfile - 2>&1 | tee -a server.log"

for i in $(seq 1 15); do
  code=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8000/ || true)
  [ "$code" = "200" ] && echo "OK: server up ($code)" && exit 0
  sleep 2
done

echo "FAILED: server did not become healthy"
exit 1