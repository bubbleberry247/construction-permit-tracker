# 建設業許可証管理システム (construction-permit-tracker)

## 概要
東海インプル建設向け。協力会社145社の建設業許可証の期限管理・通知システム。
GAS（通知・フォーム受付）+ Python（OCR・Gmail取込・Sheets登録）の2層構成。

## 技術スタック
- **エンコーディング**: UTF-8を基本とする（CSV出力はBOM付きUTF-8、コンソール出力は `-X utf8` オプション使用）
- **GAS**: 通知バッチ、フォーム受付、UI（Sheets メニュー）
- **Python 3.11+**: OCR（GPT-4o Vision）、Gmail取込、Sheets登録、MLIT確認
- **Google Sheets**: データストア（Companies / Permits / Submissions / Notifications / Config）
- **Google Forms**: 協力会社からの許可証PDF受領
- **Gmail**: 通知送信（送信元: kalimistk@gmail.com）
- **clasp**: 未設定（GASエディタ直接編集）

## ファイル構成
```
src/
├── *.gs          — GAS（Config, Utils, Models, FormHandler, Scheduler, Mailer, Ui, CompanyViewModel）
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

- 通知モデルは結果提出型（更新後に許可証を提出してもらう）
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
| DRIVE_ROOT_FOLDER_ID | 許可証PDF保管フォルダID |
| FORM_ID | Google Form ID |

## データ設計ルール（絶対）
- **マスタ = 教師データ（`145社.xlsx`）**。145社を無条件に全社登録する
- CID（会社ID）、MLIT検索結果は**補助情報**。マスタではない。登録・除外の根拠にしない
- メール受信したら教師データとメールアドレスで**完全一致**突合し、許可証データを追記する
- メールアドレスのドメイン一致での自動マッチは**禁止**（共有ドメイン誤判定の原因）
- **教師データの会社名が正**。表記ゆれは都度確認（自動正規化で同一判定しない）
- **対象メール**: shinsei.tic から kalimistk に転送されたメールのみ。直接メールは対象外
- 1社に複数メールアドレス・複数添付がある場合は全メールの添付を合わせて登録
- ダッシュボードは**全145社を常に表示**する（MLIT・CIDの有無に依存しない）

