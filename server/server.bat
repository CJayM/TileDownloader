@echo off
cd /d "%~dp0"
rem ============================================================
rem  Tile Downloader — сервер заданий (отдельный проект)
rem  Мастер-БД тайлов + jobs.db3 + HTTP API + HTML-дашборд.
rem  Запуск:   server.bat [дополнительные аргументы]
rem  Дашборд:  http://localhost:31059/dashboard
rem  Зависимости: если не установлены — сначала install.bat.
rem  Порт 31059 выбран случайно, чтобы не пересекаться с adb и др.
rem  Каталог мастер-БД: по умолчанию .\master. Продолжить существующую
rem  базу:  server.bat -o D:\tiles_out
rem ============================================================
python server.py -o .\master --host 0.0.0.0 --port 31059 --min-zoom 1 --max-zoom 14 %*
pause
