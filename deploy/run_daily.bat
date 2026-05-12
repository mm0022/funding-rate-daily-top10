@echo off
setlocal

REM Resolve project root = parent of this script's directory
set "SCRIPT_DIR=%~dp0"
pushd "%SCRIPT_DIR%.."

if not exist logs mkdir logs

REM yyyyMMdd via PowerShell (locale-independent; works on Windows 10/11)
for /f %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd"') do set "TODAY=%%I"

call .venv\Scripts\activate.bat

REM Ensure the src/ layout is importable even if the package wasn't installed via
REM `pip install -e .`. Harmless if it was — duplicate paths are deduped.
set "PYTHONPATH=%CD%\src;%PYTHONPATH%"

REM Force UTF-8 for Python IO; otherwise logger output containing emoji / em-dash
REM crashes when redirected to the log file on a gbk Windows console.
set "PYTHONIOENCODING=utf-8"

python -m funding_top10.main 1>>"logs\daily_%TODAY%.log" 2>&1
set "RC=%errorlevel%"

popd
endlocal & exit /b %RC%
