@echo off
cd /d "%~dp0"
python scripts\deploy.py up
if errorlevel 1 exit /b 1
