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
tests/            — test_core.py（110 passed）, ocr_benchmark.py
```

## 顧客・関係者
- **藤田さん** (m-fujita): 東海インプル建設の担当者。通知の最終受信者
- **古川さん**: 藤田さんと共に社内通知を受ける
- **kanri.tic**: 管理用メールアドレス（本番展開時に追加）
- **kalimistk@gmail.com**: GAS実行アカウント（現在の送信元・テスト通知先）

## 通知要件（2026-04-02 藤田さん確定）

### 運用モデル: 結果提出型
更新時期をコントロールするのではなく、**更新後に許可証を提出してもらう**モデル。
① 期限前 → システムが**藤田・古川宛に月次レポート**（自動・毎月1日）
② 藤田さんが**現場監督に継続確認**（手動）
③ 継続する業者のみ**更新依頼+PDF提出案内**（手動）
④ 未回答は**フォローまたはクローズ**（運用整理中）

### 通知タイミング
- **一律90日前**スタート（知事/大臣で分岐しない。藤田さん確認済み）
- NOTIFY_STAGES_DAYS: `90,60,30,0`

### 通知ルール
- 送信頻度: **月1回**（毎月1日に自動送信）
- 宛先: **藤田 + 古川のみ**（kanriアドレスは除外。藤田さん指定）
- 本番移行まで: **kalimistk@gmail.com のみ**に送信
- メール内容: 期限90日以内の一覧 + 「更新後は新許可証PDFをGoogleフォーム経由で提出」の案内
- メッセージトーン: 「手続きを開始してください」ではなく「更新後はご提出ください」

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

## 未実装・TODO
- [x] ~~知事/大臣の許可区分による通知タイミング分岐~~ → 一律90日前に確定（2026-04-02）
- [x] ~~週次サマリー → 月次への変更~~ → 毎月1日に実装済み
- [x] ~~個別通知メール本文にPDF提出依頼文を追加~~ → 結果提出型トーンに変更済み
- [ ] **デプロイ時必須**: ConfigシートのNOTIFY_STAGES_DAYSを `90,60,30,0` に手動更新（コードのデフォルト値変更だけでは既存設定を上書きしない）
- [ ] ADMIN_EMAILS を藤田+古川のみに変更（本番移行時）
- [ ] clasp連携（.clasp.json未設定、GASログのCLI取得不可）
- [ ] フロー④のフォロー/クローズ自動化（藤田さん側で運用整理中）

## 納品方針（2026-04-02 確定）
- **今回の納品**: 145社全社の一覧表（29業種○マトリクス）+ 添付書類一式
- **今後のアプリ運用**: 許可証が必要な会社のみ管理（MLIT起点でOK）
- 許可証なし業者は一覧表に「許可証なし」と明記
