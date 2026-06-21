@echo off
if not exist logs mkdir logs
.\.venv\Scripts\python.exe run.py > logs\startup.log 2>&1
