# 建設業許可証管理システム — セットアップガイド

東海インプル建設㈱向け 協力会社145社 建設業許可証管理システム

---

## 前提条件

| ツール | バージョン | 用途 |
|--------|-----------|------|
| Python | **3.11 以上** | OCR・Sheets登録・Gmail受信スクリプト（3.10以下は型ヒント構文非対応） |
| pip | 23 以上 | Python 同梱。`pip --version` で確認 |
| Node.js + npm | 18 以上 | clasp（GASデプロイ）|
| Google Chrome / Chromium | 最新 | Playwright が自動インストール（`playwright install chromium`）|
| Git | 任意 | リポジトリ管理 |
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

`config.json`（プロジェクトルート直下）で動作を制御します。
**APIキーや認証情報の実値はこのファイルに書かず、ファイルパスのみを記載してください。**

```json
{
  "DATA_ROOT": "C:\\ProgramData\\RK10\\Robots\\建設業許可証管理",
  "OPENAI_MODEL": "gpt-4o",
  "OPENAI_API_KEY_FILE": "認証情報フォルダ\\openai_api_key.txt",
  "GOOGLE_CREDENTIALS_FILE": "認証情報フォルダ\\google_oauth2_credentials.json",
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

### 各フィールドの説明

| フィールド | 説明 | 変更タイミング |
|---|---|---|
| `DATA_ROOT` | プロジェクトルートの絶対パス。`data/inbox/`, `data/processed/`, `output/` の基点 | 環境移行時 |
| `OPENAI_MODEL` | 使用する GPT モデル（`gpt-4o`, `gpt-4o-mini` 等）。mini はコスト削減だが精度低下あり | コスト調整時 |
| `OPENAI_API_KEY_FILE` | OpenAI APIキーが書かれたテキストファイルの絶対パス | 鍵ファイル移動時 |
| `GOOGLE_CREDENTIALS_FILE` | OAuth 2.0 credentials.json の絶対パス | 鍵ファイル移動時 |
| `GOOGLE_SHEETS_ID` | 登録先スプレッドシートの ID。**空のとき dry-run モードで動作**（書き込みなし） | スプレッドシート作成後 |
| `GMAIL_ADDRESS` | 許可証メールを受信している Gmail アドレス | アカウント変更時 |
| `GMAIL_LABEL_PROCESSED` | 処理済みメールに付与する Gmail ラベル名。ラベルが存在しない場合は自動作成 | 任意 |
| `GMAIL_FETCH_MAX` | 1回の実行で取得する最大メール件数。多すぎると処理時間・API コスト増加 | 件数調整時 |
| `OCR_CONFIDENCE_THRESHOLD_LOW` | この値未満の OCR 信頼度は `error` フォルダに振り分け（デフォルト: 0.30）。低解像度 PDF が多い場合は下げる | PDF 品質次第 |
| `OCR_CONFIDENCE_THRESHOLD_REVIEW` | この値未満の OCR 信頼度は警告ログを出力するが処理は継続（デフォルト: 0.60） | PDF 品質次第 |
| `RETRY_MAX` | API 呼び出し失敗時の最大リトライ回数（Exponential Backoff） | ネットワーク環境次第 |
| `RETRY_BASE_DELAY_SEC` | Exponential Backoff の基底待機秒数。`1.0` → 1秒, 2秒, 4秒... | ネットワーク環境次第 |
| `STAGING_CSV_DIR` | ステージング CSV の出力先サブディレクトリ（`DATA_ROOT` 相対） | 任意 |
| `LOG_LEVEL` | ログ出力レベル（`DEBUG` / `INFO` / `WARNING` / `ERROR`）。`DEBUG` で詳細ログ確認可 | デバッグ時 |

**VPN 本番環境への切り替え**: `DATA_ROOT` を `\\\\server\\建設業許可証管理` に変更するだけ。

---

## 3. Google 認証セットアップ

### 認証方式の選択について

本システムでは **Google Sheets（台帳書き込み）** と **Gmail（許可証メール受信）** の
2つの Google API を使用します。

| 用途 | 推奨認証方式 | 理由 |
|---|---|---|
| Sheets 書き込み | **OAuth 2.0（ユーザーアカウント）** | 既存の `shinsei.tic@gmail.com` アカウントで直接アクセスでき、サービスアカウントへの「共有」設定が不要 |
| Gmail 受信 | **OAuth 2.0（ユーザーアカウント）** | Gmail API はサービスアカウントで利用するには Google Workspace のドメイン委任設定が必要で、個人 Gmail アカウントでは使用不可 |

> **サービスアカウントを使わない理由**: 個人 Gmail（`@gmail.com`）の Gmail API はサービスアカウントでは
> アクセスできません。Sheets のみであれば可能ですが、設定の統一のため OAuth 2.0 に統一します。

### 手順: OAuth 2.0 credentials.json の取得

1. [Google Cloud Console](https://console.cloud.google.com/) を開く
2. プロジェクトを作成（または既存プロジェクトを選択）
3. 「APIとサービス」→「有効なAPIとサービス」で以下を有効化:
   - **Google Sheets API**
   - **Gmail API**
4. 「APIとサービス」→「認証情報」→「認証情報を作成」→「OAuth クライアント ID」
   - アプリの種類: **「デスクトップアプリ」**
5. 作成後「JSONをダウンロード」
6. ダウンロードしたファイルを `GOOGLE_CREDENTIALS_FILE` のパスに保存:
   ```
   C:\ProgramData\RK10\credentials\google_oauth2_credentials.json
   ```
7. 初回実行時にブラウザが開き、Google アカウントへの認証を求められます。
   `shinsei.tic@gmail.com` でログインすると `logs/.gmail_token.json` にトークンが保存され、
   以降は自動認証されます。

> **注意**: 同意画面が「テスト」モードの場合、使用するメールアドレスを
> 「テストユーザー」に追加してください。

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

`output/staging_permits_YYYYMMDD_HHMMSS.csv` を Excel で開き、以下を確認してください:

| チェック項目 | 期待値 |
|---|---|
| `parse_status` | `OK` / `REVIEW_NEEDED` / `SKIP`（`ERROR` が多い場合は PDF 品質や設定を確認） |
| `permit_authority_name_normalized` | `愛知県知事`, `国土交通大臣` 等の正規表記 |
| `permit_category` | `特定` または `一般` |
| `expiry_date` | `YYYY-MM-DD` 形式 |
| `company_id` | 会社マスタの ID が設定されていること |
| `trade_categories` | `電気工事業|管工事業` 等（`|` 区切り） |

`parse_status=REVIEW_NEEDED` の主な原因:
- `TIER3_UNKNOWN_COMPANY`: Companies マスタに会社が未登録 → `import_company_master.py` で再取込または手動で `company_id` を入力
- `TIER3_DATE_PARSE`: 有効期限のパース失敗 → `expiry_date` 列を手動修正
- `TIER3_TRADE_MISMATCH`: 業種名の正規化失敗 → `src/utils/trade_master.py` の `TRADE_ALIASES` に追加

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
| `FileNotFoundError: config.json` | カレントディレクトリがプロジェクトルートでない | `cd <project_root>` してから実行 |
| `FileNotFoundError: APIキーファイルが見つかりません` | `OPENAI_API_KEY_FILE` のパスが間違い | ファイルを作成: `echo sk-... > <パス>` |
| `ImportError: gspread` | pip install 未完了 | `pip install -r requirements.txt` |
| `google.auth.exceptions.DefaultCredentialsError` | credentials.json が見つからない | `GOOGLE_CREDENTIALS_FILE` のパスを確認 |
| Google 認証エラー（401/403） | OAuth 認証失敗またはトークン期限切れ | `logs/.gmail_token.json` を削除して再認証。同意画面「テスト」モードの場合はテストユーザーにメールアドレスを追加 |
| `PDF が暗号化されています` | パスワード付き PDF | PDF のパスワードを解除してから `data/inbox/` に配置 |
| `OCR信頼度が低すぎます: 0.XX < 0.30` | スキャン品質が低い | 300 dpi 以上で再スキャン、または `OCR_CONFIDENCE_THRESHOLD_LOW` を下げる |
| `許可番号のパターンに一致しませんでした` | OCR テキストに許可番号が含まれない | `data/processed/review_needed/` の PDF を目視確認 |
| `BLOCKED: TIER3_UNKNOWN_COMPANY` | Companies マスタに会社が未登録 | `import_company_master.py` で再取込 or `company_id` を手動入力 |
| `APIError: 429` | Sheets API レート制限 | `RETRY_BASE_DELAY_SEC` を上げて（例: 2.0）再実行 |
| `playwright._impl._errors.Error` | Playwright ブラウザ未インストール | `playwright install chromium` |
| プロキシ環境で `playwright install` が失敗 | プロキシ設定が必要 | `set HTTPS_PROXY=http://proxy:8080` を設定してから実行 |

### テストの実行

ユニットテストでコアロジックの動作を確認できます:

```bash
cd <project_root>
pytest tests/test_core.py -v
```

期待結果: `110 passed`

---

## 9. clasp 開発フロー（GASコード変更時）

```bash
npm run push   # ローカル → GAS に反映
npm run pull   # GAS → ローカルに取得
npm run open   # ブラウザでスクリプトエディタを開く
```
