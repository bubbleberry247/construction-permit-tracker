# 建設業許可証管理システム (construction-permit-tracker)

## 概要
東海インダストリアルコンサルティング向け。Excel原本127社を候補とする建設業許可証の期限管理・通知システム。
現行MVPはGAS Webアプリでの会社マスタ更新、MLIT観測、担当者確認後の通知送信を対象とする。

## 技術スタック
- **エンコーディング**: UTF-8を基本とする（CSV出力はBOM付きUTF-8、コンソール出力は `-X utf8` オプション使用）
- **GAS**: Webアプリ、会社マスタ更新、MLIT観測、通知候補・送信ゲート
- **Python 3.11+**: OCR（GPT-4o Vision）、Gmail取込、Sheets登録、MLIT確認
- **Google Sheets**: データストア（Companies / Permits / MLITPermits / NotificationQueue / Notifications / UserAccess / AuditLog）
- **Gmail**: 通知送信（送信元: kalimistk@gmail.com）
- **clasp**: 未設定（GASエディタ直接編集）

## ファイル構成
```
src/
├── *.gs          — GAS（認証、会社マスタ、MLIT観測、通知キュー、送信ゲート、Web UI）
├── *.py          — Python（fetch_gmail, ocr_permit, import_company_master, register_sheets, mlit_confirm）
└── utils/        — permit_parser, trade_master, wareki_convert
scripts/          — run_pipeline.bat, setup_task_scheduler.ps1
tests/            — test_core.py, ocr_benchmark.py
```

## 顧客・関係者
- **藤田さん** (m-fujita): 東海インプル建設の担当者。通知の最終受信者
- **古川さん**: 藤田さんと共に社内通知を受ける
- **kanri.tic**: 管理用メールアドレス（本番展開時に追加）
- **kalimistk@gmail.com**: GAS実行アカウント（現在の送信元・テスト通知先）

## 運用制約

- 通知モデルは許可期限の確認・更新依頼型。システム内の外部受付機能は持たない
- 通知ステージは `90,60,30,0`（一律90日前スタート）
- 宛先・送信可否などの実値は `Config` シートが source of truth
- コードの default 変更では既存 `Config` シートの値を上書きしない

## Config シート 主要キー
| key | 説明 |
|---|---|
| ADMIN_EMAILS | 通知先メール（カンマ区切り） |
| NOTIFY_STAGES_DAYS | 通知ステージ日数（90,60,30,0） |
| ENABLE_SEND | true=送信 / false=ドライラン |
| GMAIL_DAILY_LIMIT | 日次送信上限（デフォルト150） |
| MLIT_SYNC_MODE | OFF / SHADOW / MANUAL_APPLY |

## データ設計ルール（絶対）
- **会社マスタ候補 = Excel原本127社**。照合・承認前に本番Companiesへ自動反映しない
- CID（会社ID）、MLIT検索結果は**補助情報**。マスタではない。登録・除外の根拠にしない
- メール受信したら教師データとメールアドレスで**完全一致**突合し、許可証データを追記する
- メールアドレスのドメイン一致での自動マッチは**禁止**（共有ドメイン誤判定の原因）
- **教師データの会社名が正**。表記ゆれは都度確認（自動正規化で同一判定しない）
- **対象メール**: shinsei.tic から kalimistk に転送されたメールのみ。直接メールは対象外
- 1社に複数メールアドレス・複数添付がある場合は全メールの添付を合わせて登録
- ダッシュボードは承認済みCompaniesを起点に表示し、`permit_monitoring_enabled`で許可管理対象を決定する

