@echo off
REM ============================================================================
REM nightly_mlit_rolling.bat
REM   建設業許可証管理 - MLIT 毎晩ローリング再確認
REM   - daily_limit 件ずつ巡回（145社を約1ヶ月で1巡）
REM   - max_stale_days 日以上未確認 or NOT_CONFIRMED の会社が対象
REM   - kill switch: c:\tmp\mlit_pause を作成すれば即停止
REM
REM Task Scheduler 登録:
REM   - 起動: 毎日 02:00（業務時間外）
REM   - ユーザーがログオンしているか否かにかかわらず実行
REM   - 最上位の特権で実行（不要だが推奨）
REM ============================================================================

setlocal
set "PROJECT_DIR=c:\ProgramData\Generative AI\Github\construction-permit-tracker"
set "LOG_DIR=%PROJECT_DIR%\logs"
set "PYTHON_EXE=python"

REM 日付付きログファイル
for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value ^| find "="') do set "DT=%%I"
set "LOG_FILE=%LOG_DIR%\nightly_mlit_%DT:~0,8%.log"

REM ログディレクトリを作成
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

REM kill switch 事前チェック（ログに残す）
if exist "c:\tmp\mlit_pause" (
    echo [%DATE% %TIME%] kill switch detected - skip nightly MLIT >> "%LOG_FILE%"
    exit /b 0
)

cd /d "%PROJECT_DIR%"

REM rolling 実行: daily_limit=5 / max_stale_days=30
REM   145 社 / 5 件 = 29 日で全件巡回
echo [%DATE% %TIME%] start nightly MLIT rolling >> "%LOG_FILE%"
"%PYTHON_EXE%" src\mlit_confirm.py --rolling --daily-limit 5 --max-stale-days 30 >> "%LOG_FILE%" 2>&1
set "RC=%ERRORLEVEL%"
echo [%DATE% %TIME%] end nightly MLIT rolling rc=%RC% >> "%LOG_FILE%"

exit /b %RC%
