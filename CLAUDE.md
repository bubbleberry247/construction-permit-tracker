# 建設業許可証管理システム (construction-permit-tracker)

## 概要
東海インプル建設向け。協力会社145社の建設業許可証の期限管理・通知システム。
GAS（通知・フォーム受付）+ Python（OCR・Gmail取込・Sheets登録）の2層構成。

## 技術スタック
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
tests/            — test_core.py（110 passed）, ocr_benchmark.py
```

## 顧客・関係者
- **藤田さん** (m-fujita): 東海インプル建設の担当者。通知の最終受信者
- **古川さん**: 藤田さんと共に社内通知を受ける
- **kanri.tic**: 管理用メールアドレス（本番展開時に追加）
- **kalimistk@gmail.com**: GAS実行アカウント（現在の送信元・テスト通知先）

## 通知要件（2026-03-31 藤田さん確定）

### 運用フロー
① 期限前 → システムが**藤田・古川宛に社内通知**（自動・月1回）
② 藤田さんが**現場監督に継続確認**（手動）
③ 継続する業者のみ**更新依頼+PDF提出案内**（手動）
④ 未回答は**フォローまたはクローズ**（運用整理中）

### 通知タイミング（許可区分で異なる）
| 許可区分 | 初回通知 | 標準処理期間 | 根拠 |
|---|---|---|---|
| 知事許可 | 満了**90日前** | 23営業日（実務1.5〜2か月） | [愛知県行政手続情報](https://www.pref.aichi.jp/site/gyoute/3600.html) |
| 大臣許可 | 満了**180日前** | おおむね120日（移送30日+審査90日） | [国交省通達 国総建第99号](https://www.mlit.go.jp/common/000004787.pdf) |

### 通知ルール
- 送信間隔: **月1回**（社内レポート）
- 本番移行まで: **kalimistk@gmail.com のみ**に送信
- 本番後: 藤田さん + 古川さん宛
- メール内容: 期限接近一覧 + 「更新後は新許可証PDFをGoogleフォーム経由で提出」の案内

## Config シート 主要キー
| key | 説明 |
|---|---|
| ADMIN_EMAILS | 通知先メール（カンマ区切り） |
| NOTIFY_STAGES_DAYS | 通知ステージ日数（現行: 120,90,60,45,30,14,0） |
| ENABLE_SEND | true=送信 / false=ドライラン |
| GMAIL_DAILY_LIMIT | 日次送信上限（デフォルト150） |
| DRIVE_ROOT_FOLDER_ID | 許可証PDF保管フォルダID |
| FORM_ID | Google Form ID |

## 未実装・TODO
- [ ] 知事/大臣の許可区分による通知タイミング分岐（Scheduler.gs）
- [ ] 週次サマリー → 月次（10日間隔）への変更
- [ ] 個別通知メール本文にPDF提出依頼文を追加（Mailer.gs）
- [ ] ADMIN_EMAILS を藤田+古川のみに変更（本番移行時）
- [ ] clasp連携（.clasp.json未設定、GASログのCLI取得不可）
- [ ] フロー④のフォロー/クローズ自動化（藤田さん側で運用整理中）
