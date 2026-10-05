@echo off
cd /d "%~dp0"
rem Установка зависимостей сервера (aiohttp).
python -m pip install -r requirements.txt
pause
