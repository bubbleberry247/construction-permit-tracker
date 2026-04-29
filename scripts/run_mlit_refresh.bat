@echo off
REM MLIT 許可情報を MLITPermits シートに同期する日次実行スクリプト
REM Task Scheduler から呼び出される (毎日 03:30 JST)

cd /d "C:\ProgramData\Generative AI\Github\construction-permit-tracker"

if not exist "logs\mlit_refresh" mkdir "logs\mlit_refresh"

set LOG_FILE=logs\mlit_refresh\mlit_refresh_%date:~0,4%%date:~5,2%%date:~8,2%.log

echo === MLIT Refresh Start: %date% %time% === >> "%LOG_FILE%"
python scripts\refresh_mlit_permits_python.py --execute >> "%LOG_FILE%" 2>&1
echo === MLIT Refresh End: %date% %time% (exit=%ERRORLEVEL%) === >> "%LOG_FILE%"

exit /b %ERRORLEVEL%
