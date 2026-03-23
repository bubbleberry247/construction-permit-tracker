# 建設業許可証管理システム — セットアップガイド

東海インプル建設㈱向け 協力会社145社 建設業許可証管理システム

---

## 前提条件

| ツール | バージョン | 用途 |
|--------|-----------|------|
| Python | 3.11 以上 | OCR・Sheets登録・Gmail受信スクリプト |
| Node.js + npm | 18 以上 | clasp（GASデプロイ）|
| Google Chrome | 最新 | Playwright MLIT確認 |
| VPN接続 | — | VPNサーバへのファイル保存（本番環境） |

---

## 1. Python 環境セットアップ

```bash
# 依存パッケージインストール
pip install -r requirements.txt

# Playwright ブラウザインストール（MLIT確認に必要）
playwright install chromium
```

---

## 2. config.json の設定

`config.json` の各フィールドを環境に合わせて編集してください:

```json
{
  "DATA_ROOT": "C:\\ProgramData\\RK10\\Robots\\建設業許可証管理",
  "OPENAI_MODEL": "gpt-4o",
  "OPENAI_API_KEY_FILE": "認証情報フォルダ\\openai_api_key.txt",
  "GOOGLE_CREDENTIALS_FILE": "認証情報フォルダ\\google_service_account.json",
  "GOOGLE_SHEETS_ID": "スプレッドシートのID（URLの /d/ と /edit の間）",
  "GMAIL_ADDRESS": "shinsei.tic@gmail.com",
  "GMAIL_LABEL_PROCESSED": "許可証処理済み",
  "GMAIL_FETCH_MAX": 50,
  "OCR_CONFIDENCE_THRESHOLD_LOW": 0.30,
  "OCR_CONFIDENCE_THRESHOLD_REVIEW": 0.60,
  "RETRY_MAX": 3,
  "RETRY_BASE_DELAY_SEC": 1.0,
  "STAGING_CSV_DIR": "output",
  "LOG_LEVEL": "INFO"
}
```

**VPN本番環境への切り替え**: `DATA_ROOT` を `\\\\server\\建設業許可証管理` に変更するだけ。

---

## 3. Google 認証セットアップ

### 3-A: Google Sheets 用 Service Account

1. [Google Cloud Console](https://console.cloud.google.com/) でプロジェクトを開く
2. 「APIとサービス」→「認証情報」→「サービスアカウント作成」
3. ロール: `編集者` (Sheets書き込みに必要)
4. キー作成（JSON形式）→ `GOOGLE_CREDENTIALS_FILE` のパスに保存
5. 作成したサービスアカウントのメールアドレスを Google Spreadsheet の「共有」に追加

### 3-B: Gmail 用 OAuth2（fetch_gmail.py）

1. Google Cloud Console で「Gmail API」を有効化
2. 「OAuth 2.0 クライアント ID」を作成（デスクトップアプリ）
3. `credentials.json` をダウンロード → `GOOGLE_CREDENTIALS_FILE` とは**別のパス**に保存
4. `fetch_gmail.py` の `credentials_file` 引数を OAuth2 のパスに指定
5. 初回実行時にブラウザ認証画面が開く → `shinsei.tic@gmail.com` でログイン → トークンが `logs/.gmail_token.json` に保存

> **注意**: Service Account（Sheets用）と OAuth2（Gmail用）は別ファイルです。

---

## 4. Google Sheets 初期化（GAS側）

### 4-A: clasp でGASをデプロイ

```bash
npm install
npx clasp login

# .clasp.json にスクリプトIDを設定
cp .clasp.json.template .clasp.json
# .clasp.json の YOUR_SCRIPT_ID_HERE を実際のIDに書き換える

npm run push
```

### 4-B: GAS側の初期設定

スクリプトエディタ（または Sheets メニュー「許可証管理」）から:

1. **`initSheetHeaders()`** を実行 → 全シート（Companies/Permits/Notifications等）のヘッダ初期化
2. **Config シートに設定値を入力**:
   - `ADMIN_EMAILS` : 管理者通知先（例: `fujita@example.com`）
   - `FORM_ID` : 許可証提出 Google Form のID
   - `NOTIFY_STAGES_DAYS` : `120,90,60,45,30,14,0`（デフォルト）
   - `ENABLE_SEND` : `false`（テスト中）→ 本番は `true`
3. **`checkConfigMenu()`** → 「設定チェック OK」を確認
4. **`sendTestEmail('自分のメール')`** → テストメール受信確認
5. **`setupDailyTrigger()`** → 日次バッチ（毎朝8時）のトリガーを設定
6. `ENABLE_SEND` を `true` に変更 → 本番稼働

---

## 5. 初回実行手順（3月末フロー）

### Step 0: 会社マスタ取込（最初の1回のみ）

```bash
python src/import_company_master.py --xlsx "継続取引業者リスト.xlsx"
```

Companies シートに145社が登録されることを確認。

### Step 1: テストPDFで動作確認

```bash
# samples/ にサンプルPDFをコピーして inbox/ に置く
copy samples\test_permit.pdf data\inbox\

# OCR + GPT-4o 抽出（dry-run で対象確認）
python src/ocr_permit.py --dry-run

# 実行
python src/ocr_permit.py
```

### Step 2: staging CSV を目視確認・修正

`output/staging_YYYYMMDD.csv` を Excel で開き:
- `parse_status=REVIEW_NEEDED` の行を確認・修正
- `TIER3_UNKNOWN_COMPANY` は Companies シートに会社を追加してから `company_id` を入力

### Step 3: Google Sheets 台帳登録

```bash
# dry-run で確認（Sheetsへの書き込みなし）
python src/register_sheets.py --dry-run

# 実行
python src/register_sheets.py
```

---

## 6. 自動化セットアップ（4月以降）

### Windows タスクスケジューラへの登録

```powershell
# 管理者PowerShellで実行
powershell -ExecutionPolicy Bypass -File scripts\setup_task_scheduler.ps1
```

毎朝7時に `fetch_gmail.py → ocr_permit.py → register_sheets.py` が自動実行されます。

### 手動実行

```bash
scripts\run_pipeline.bat
```

---

## 7. MLIT etsuran2 確認（4月以降）

```bash
# 対象: EXPIRING / RENEWAL_OVERDUE / EXPIRED / RENEWAL_IN_PROGRESS
python src/mlit_confirm.py --dry-run   # URL確認のみ
python src/mlit_confirm.py             # Playwright実行 + スクリーンショット保存
```

スクリーンショットは `data/mlit_screenshots/YYYYMMDD_業者番号.png` に保存されます。

---

## 8. トラブルシューティング

| エラー | 原因 | 対処 |
|--------|------|------|
| `FileNotFoundError: config.json` | config.json が PROJECT_ROOT にない | `config.json` を編集してパスを確認 |
| `ImportError: gspread` | pip install 未完了 | `pip install -r requirements.txt` |
| `google.auth.exceptions.DefaultCredentialsError` | Service Account JSONが見つからない | `GOOGLE_CREDENTIALS_FILE` のパスを確認 |
| `BLOCKED: TIER3_UNKNOWN_COMPANY` | Companies マスタに会社が未登録 | `import_company_master.py` で再取込 or 手動追加 |
| `APIError: 429` | Sheets API レート制限 | `RETRY_BASE_DELAY_SEC` を上げて再実行 |
| Gmail OAuth2 認証エラー | トークン期限切れ | `logs/.gmail_token.json` を削除して再認証 |
| `playwright._impl._errors.Error` | Playwrightブラウザ未インストール | `playwright install chromium` |

---

## 9. clasp 開発フロー（GASコード変更時）

```bash
npm run push   # ローカル → GAS に反映
npm run pull   # GAS → ローカルに取得
npm run open   # ブラウザでスクリプトエディタを開く
```
