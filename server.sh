#!/bin/sh
# ============================================================
#  Tile Downloader — сервер
#  Мастер-БД тайлов + jobs.db3 + HTTP API + HTML-дашборд.
#  Запуск:  ./server.sh [дополнительные аргументы]
#  Дашборд: http://localhost:8080/dashboard
# ============================================================
python3 server.py -o ./master --host 0.0.0.0 --port 8080 --min-zoom 1 --max-zoom 14 "$@"
