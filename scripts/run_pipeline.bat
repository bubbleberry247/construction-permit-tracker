@echo off
rem ============================================================
rem 建設業許可証管理システム — 自動パイプライン実行スクリプト
rem Windowsタスクスケジューラから呼び出す（毎朝 07:00 推奨）
rem
rem 前提条件:
rem   - Python（仮想環境 .venv） がセットアップ済み
rem   - VPN接続済み（DATA_ROOT がUNCパスの場合）
rem   - Google OAuth2 初回認証済み（logs/.gmail_token.json が存在）
rem ============================================================

setlocal

rem プロジェクトルート（このスクリプトの1つ上のディレクトリ）
set "PROJECT_ROOT=%~dp0.."
cd /d "%PROJECT_ROOT%"

rem Python仮想環境をアクティベート（存在する場合）
if exist ".venv\Scripts\activate.bat" (
    call ".venv\Scripts\activate.bat"
)

rem ログファイル（日付付き）
set "LOG_DIR=%PROJECT_ROOT%\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set "LOG_FILE=%LOG_DIR%\pipeline_%DATE:~0,4%%DATE:~5,2%%DATE:~8,2%.log"

echo ========================================= >> "%LOG_FILE%"
echo 実行開始: %DATE% %TIME% >> "%LOG_FILE%"
echo ========================================= >> "%LOG_FILE%"

rem Step 1: Gmail から PDF 受信
echo [Step 1] Gmail PDF受信... >> "%LOG_FILE%"
python src\fetch_gmail.py >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo [ERROR] fetch_gmail.py が失敗しました。 >> "%LOG_FILE%"
    goto :end
)

rem Step 2: OCR + GPT-4o 抽出 → staging CSV
echo [Step 2] OCR処理... >> "%LOG_FILE%"
python src\ocr_permit.py >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo [ERROR] ocr_permit.py が失敗しました。 >> "%LOG_FILE%"
    goto :end
)

rem Step 3: staging CSV → Sheets 登録
rem 注意: REVIEW_NEEDED は自動スキップ（手動確認後に別途実行）
echo [Step 3] Sheets登録... >> "%LOG_FILE%"
python src\register_sheets.py >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo [ERROR] register_sheets.py が失敗しました。 >> "%LOG_FILE%"
    goto :end
)

echo 完了: %DATE% %TIME% >> "%LOG_FILE%"

:end
endlocal
