# MLIT 毎晩ローリング再確認 - Task Scheduler 登録手順

## 概要

`scripts/nightly_mlit_rolling.bat` を Windows Task Scheduler で毎晩 02:00 に起動し、
145 社の建設業許可をローリング再確認する。

- 1 日あたり 5 件処理（約 1〜2 分）
- 145 社を約 29 日で 1 巡
- `last_confirmed_at` が 30 日以上前 or `NOT_CONFIRMED` の会社が優先対象

## 設計の根拠（GPT-5.4 レビュー反映済み）

| 観点 | 採用方針 |
|------|----------|
| MLIT 規約 | 1 件ごと 3 秒ウェイト + 単一プロセス（`headless=True`、Playwright 同期） |
| kill switch | `c:\tmp\mlit_pause` ファイル存在で即停止（Bat 冒頭 + Python ループ内 + FastAPI ハンドラ） |
| 同時実行制御 | Task Scheduler の "新しいインスタンスを開始しない" 設定で重複起動防止 |
| 失敗ログ | `logs\nightly_mlit_YYYYMMDD.log` に毎日追記、`mlit_confirmation_log.csv` に証跡 |
| データ反映 | `companies.mlit_status / last_confirmed_at` を直接更新（Sheets には FastAPI 経由で別途反映可能） |

## 登録手順（GUI）

1. `Win + R` → `taskschd.msc` で Task Scheduler を起動
2. 「タスクの作成」を選択（基本タスクではなく **作成**）
3. **全般** タブ:
   - 名前: `MLIT_Nightly_Rolling`
   - 説明: `建設業許可145社の毎晩ローリング再確認（5件/日）`
   - "ユーザーがログオンしているかどうかにかかわらず実行する" を選択
   - "最上位の特権で実行する" にチェック（推奨）
4. **トリガー** タブ → 新規:
   - 開始: 毎日 `02:00`
   - 詳細設定 → "有効" にチェック
5. **操作** タブ → 新規:
   - 操作: プログラムの開始
   - プログラム/スクリプト: `c:\ProgramData\Generative AI\Github\construction-permit-tracker\scripts\nightly_mlit_rolling.bat`
   - 開始 (オプション): `c:\ProgramData\Generative AI\Github\construction-permit-tracker`
6. **条件** タブ:
   - "コンピューターを AC 電源で使用している場合のみ" → 不要なのでチェック解除
   - "タスクを実行するためにスリープを解除する" にチェック
7. **設定** タブ:
   - "タスクの実行が要求された時に既にタスクが実行している場合の規則" → **新しいインスタンスを開始しない**
   - "タスクが次の時間以上長く実行された場合は停止する" → 30 分

## 登録手順（PowerShell, 管理者）

```powershell
$action = New-ScheduledTaskAction `
    -Execute "c:\ProgramData\Generative AI\Github\construction-permit-tracker\scripts\nightly_mlit_rolling.bat" `
    -WorkingDirectory "c:\ProgramData\Generative AI\Github\construction-permit-tracker"

$trigger = New-ScheduledTaskTrigger -Daily -At 2:00am

$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 30) `
    -WakeToRun

Register-ScheduledTask -TaskName "MLIT_Nightly_Rolling" `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -User "SYSTEM" `
    -RunLevel Highest `
    -Description "建設業許可145社の毎晩ローリング再確認（5件/日）"
```

## 動作確認

### 手動実行
```cmd
"c:\ProgramData\Generative AI\Github\construction-permit-tracker\scripts\nightly_mlit_rolling.bat"
```

### dry-run（Playwright 非実行）
```cmd
cd /d "c:\ProgramData\Generative AI\Github\construction-permit-tracker"
python src\mlit_confirm.py --rolling --dry-run --daily-limit 3 --max-stale-days 30
```

### kill switch のテスト
```cmd
echo. > c:\tmp\mlit_pause
python src\mlit_confirm.py --rolling --dry-run
REM → 即時 exit 75 で停止することを確認
del c:\tmp\mlit_pause
```

## 緊急停止

MLIT サイトに迷惑をかけている疑いがある場合:

```cmd
echo. > c:\tmp\mlit_pause
```

このファイルが存在する間:
- バッチは起動しても 0 件処理して終了
- ローリング再確認はループ内でも検出して中断
- FastAPI の `POST /api/companies/{cid}/mlit_refresh` も 503 を返す

再開:
```cmd
del c:\tmp\mlit_pause
```

## 監視ポイント

- `logs\nightly_mlit_YYYYMMDD.log` を週次で確認（rc=0 が続いていれば OK）
- `mlit_confirmation_log.csv` の `result` で MISMATCH / ERROR が増えていないか
- 30 日経っても巡回が完了しない会社が出たら `--daily-limit` を増やすか手動補助

## 関連ファイル

- `scripts/nightly_mlit_rolling.bat` - Task Scheduler から呼ばれる起動スクリプト
- `src/mlit_confirm.py --rolling` - 実体のローリング再確認ロジック
- `scripts/mlit_refresh_one.py` - 単独会社再確認 CLI（FastAPI から呼ばれる）
- `src/app/routers/mlit.py` - 担当者ボタン用 API エンドポイント
