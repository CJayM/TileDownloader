@echo off
rem ============================================================
rem  Tile Downloader — сервер
rem  Мастер-БД тайлов + jobs.db3 + HTTP API + HTML-дашборд.
rem  Запуск:  server.bat [дополнительные аргументы]
rem  Дашборд: http://localhost:8080/dashboard
rem ============================================================
python server.py -o .\master --host 0.0.0.0 --port 8080 --min-zoom 1 --max-zoom 14 %*
pause
