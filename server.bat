@echo off
rem ============================================================
rem  Tile Downloader — сервер
rem  Мастер-БД тайлов + jobs.db3 + HTTP API + HTML-дашборд.
rem  Запуск:  server.bat [дополнительные аргументы]
rem  Дашборд: http://localhost:8765/dashboard
rem  Порт 8765: 8080/8081 на этой машине заняты adb-сервером (127.0.0.1),
rem  который перехватывает loopback-соединения.
rem ============================================================
python server.py -o .\master --host 0.0.0.0 --port 8765 --min-zoom 1 --max-zoom 14 %*
pause
