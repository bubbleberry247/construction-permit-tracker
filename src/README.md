# src/ — バックエンドソースコード

[← プロジェクトルートに戻る](../README.md)

## 概要

建設業許可証管理システムのバックエンドPythonコード一式。
- **FastAPI Webアプリ** (`app/`) — ダッシュボード・API・画像ビューワー
- **パイプラインスクリプト** — Gmail受信→OCR→照合→MLIT検証→通知の各ステップ
- **GAS連携スクリプト** (`.gs`) — Google Sheets/Forms向けApps Scriptソース
- **ユーティリティ** (`utils/`) — OCR後処理・フィールド正規化・和暦変換

サブディレクトリの詳細は各READMEを参照。

---

## ディレクトリ構成

| パス | 内容 |
|------|------|
| [`app/`](app/README.md) | FastAPI アプリケーション本体 |
| [`utils/`](utils/README.md) | 共通ユーティリティ（OCR後処理・日本語正規化） |

---

<!-- AUTO-GENERATED: file listing -->
## ファイル一覧

### データベース・基盤

| ファイル | 概要 |
|----------|------|
| `db.py` | SQLite 正本 — `get_connection()` / `init_db()` |

### パイプライン（受信→照合→検証）

| ファイル | 概要 |
|----------|------|
| `fetch_gmail.py` | Gmail から許可証PDF添付ファイルを自動受信し `data/inbox/` に保存 |
| `extract_inbox.py` | `data/inbox/` のZIP/LZH展開・企業マッチング |
| `ocr_permit.py` | 建設業許可証PDF OCR処理 |
| `reconcile.py` | 145社マスタと受信データ（メール・PDF）の突合統合 |
| `mlit_batch_fetch.py` | MLIT全社スクレイピング + 分析 + Excel + DB更新 |
| `mlit_confirm.py` | MLIT個別確認 |
| `verify_permits_full.py` | DB全許可証を国交省APIで検証・照合 |
| `verify_permits_mlit.py` | MLIT照合サブセット検証 |

### 出力・通知

| ファイル | 概要 |
|----------|------|
| `notify_expiry.py` | 建設業許可期限通知（開発・テスト用） |
| `export_excel.py` | 管理台帳Excel出力（4シート構成） |
| `export_145_ledger.py` | 145社台帳エクスポート |
| `export_vendor_ledger.py` | 業者別台帳エクスポート |
| `generate_checklist.py` | 必要書類チェックリスト生成 |

### マスタ・データ管理

| ファイル | 概要 |
|----------|------|
| `generate_company_master.py` | 会社マスタ生成 |
| `import_company_master.py` | 会社マスタインポート |
| `rebuild_company_master.py` | 会社マスタ再構築 |
| `migrate_company_master.py` | 会社マスタマイグレーション |
| `migrate.py` | DBマイグレーション汎用 |

### 品質・監査

| ファイル | 概要 |
|----------|------|
| `evidence_ledger.py` | パイプライン全操作の監査証跡台帳 |
| `rpa_drift_watchdog.py` | パイプライン品質ドリフト検知ウォッチドッグ |
| `session_briefing.py` | セッション開始時のシステム状態サマリー生成 |
| `wiki_lint.py` | Wikiドキュメント lint |

### 補正・修正系

| ファイル | 概要 |
|----------|------|
| `fix_db_issues.py` | DB不整合修正 |
| `fix_excess_trades.py` | 業種データ過剰登録修正 |
| `fix_mlit_permits.py` | MLIT許可証データ修正 |
| `fix_unmatched_emails.py` | 未マッチメール修正 |
| `sync_ocr_overrides.py` | OCR上書き同期 |
| `normalize_pdf_rotation.py` | PDF回転正規化 |
| `verify_rotation.py` | 回転検証 |
| `recheck_rotation.py` | 回転再チェック |
| `reclassify_pages.py` | ページ分類再実行 |
| `recollect_gmail.py` | Gmail再収集 |
| `backfill_sender_emails.py` | 送信者メールアドレス補完 |

### 閲覧・調査

| ファイル | 概要 |
|----------|------|
| `staging_viewer.py` | ページ種類タグ付け + OCR編集ビューワー |
| `sync_to_sheets.py` | MLIT取得データをGoogle Sheetsに同期 |
| `register_sheets.py` | シート登録 |
| `check_integrity.py` | データ整合性チェック |
| `fetch_all_145.py` | 145社全件取得 |
| `reconcile.py` | 照合モジュール |

### GAS (Google Apps Script)

| ファイル | 概要 |
|----------|------|
| `Config.gs` | 設定値管理 |
| `Utils.gs` | 共通ユーティリティ |
| `Models.gs` | データモデル定義 |
| `FormHandler.gs` | フォーム送信処理 |
| `Scheduler.gs` | 日次通知バッチ |
| `Mailer.gs` | メール送信 |
| `Ui.gs` | スプレッドシートメニュー |
| `MlitSearch.gs` | 国交省建設業者検索API連携 |
| `db.gs` | Sheetsデータ層 |
| `logic.gs` | ビジネスロジック |
| `api.gs` | API公開関数 |
| `auth.gs` | 認証 |
| `Code2.gs` | 追加コード |
| `CompanyViewModel.gs` | 会社ビューモデル |
| `fix_notfound.gs` | 未発見データ修正 |
| `appsscript.json` | GASマニフェスト |
| `index.html` | GAS Webアプリ HTML |
<!-- END AUTO-GENERATED -->
