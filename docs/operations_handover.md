# 運用引き渡しドキュメント (2026-04-27、2026-04-29 Phase R 追記)

> この文書は旧145社・Phase R運用の履歴です。2026-07-30以降の会社マスタ127社候補、Web更新、MLIT差分、通知キューの現行設計は`company_master_notification_implementation_record_20260730.md`と`mlit_nightly_scheduler.md`を正とします。

## 経緯

藤田さん向け納品物 (協力会社書類管理 ZIP) の生成は **2026-04-27 で 1 次完結** しましたが、
4/28 に藤田さんから集計差異の指摘を受け、**継続的協同編集ワークフロー (Phase R)** に方針転換しました。
以降の継続管理は **GAS Webアプリ (MLITPermits シート + ダッシュボード)** + **Phase R 担当者編集ワークフロー** で行います。

## Phase R: 担当者編集ワークフロー (2026-04-29 〜)

藤田さんが直接編集できる Excel ワークシートを提供し、修正内容を当方で機械的に反映する仕組み。
GPT-5.5 レビュー反映の安全設計 (workflow_id 二重反映防止 / 機械列保護 / staging 経由 / snapshot rollback)。

### 着手前バックアップ (R-0、必須)
```bash
python scripts/snapshot_backup.py
git tag phase_r_pre_<TS>
```

### 担当者向けシート生成 (R-1, R-2)
```bash
python scripts/build_fujita_mapping.py <藤田 xlsx>      # 表記揺れ突合せ
python scripts/build_editor_workbook.py                  # 6 シート編集ワークブック
```
出力:
- `output/FDE_MANAGED/05_藤田マッピング_<TS>.xlsx`
- `output/FDE_MANAGED/06_担当者編集_<TS>_<wf>.xlsx`

### 反映 (R-3)
```bash
python scripts/apply_editor_decisions.py <藤田編集後 xlsx>            # dry-run 必須
python scripts/apply_editor_decisions.py <藤田編集後 xlsx> --execute  # 反映
```
- 原本 v2 は触らず、新版 `00_一覧表_v3_<TS>_<wf>.xlsx` を生成
- 新規候補は `companies_staging` テーブル (本体 companies に直接 INSERT 禁止)
- 反映履歴は `decisions_log` テーブル (workflow_id で二重反映防止)

### Rollback
```bash
python scripts/apply_editor_decisions.py --rollback <TS>
```
snapshot 復元方式。GAS スプレッドシートのみ手動復元 (Sheets バージョン履歴)。

### 詳細ドキュメント
- `docs/editor_workflow_for_fujita.md` (藤田さん向け使い方)
- `docs/reply_to_fujita_20260429.md` (返信メール下書き)
- 設計プラン: `~/.claude/plans/a-gpt5-5-lexical-pixel.md`

## 最終納品物 (2026-04-27)

| ファイル | 内容 |
|---|---|
| `output/delivery_zips/FDE_MANAGED_20260427.zip` | 最新納品 ZIP (251 files、317.6MB、build_delivery_folder.py 修正後) |
| `output/FDE_MANAGED/00_一覧表_v2_20260427.xlsx` | マスタ145社受領状況一覧 |
| `output/FDE_MANAGED/00_突合せレポート_v2_20260427.xlsx` | マスタvsDB突合せ |
| `docs/delivery_email_template.md` | 藤田さん返信メール下書き |

## 最終数字

```
マスタ145社内訳 (Stage 1-4 完了後):
  ◎ 全書類揃い:    39 社
  △ 一部受領:      43 社
  × 未受領:        38 社 (DB登録あり書類なし)
  - 当方未登録:    25 社 (マスタ記載あり、当方記録なし)
  合計:           145 社  ✓ マスタ件数と一致
```

## 継続管理方針

### Webアプリ (GAS) で管理する内容

- 各社の **建設業許可証** の種類・有効期限
- ダッシュボード: 期限切迫順表示 (logic.gs `buildDashboardData_()` で MLITPermits を表示)
- MLIT 自動更新: 毎日 02:00 JST に GAS time-driven trigger 実行 (`MlitRolling.gs`)

### Webアプリ URL

GAS Web App: 詳細は `memory/reference_gas_urls.md` を参照
データソース: Google Sheets `MLITPermits` シート (52 社の許可情報)

### Webアプリ管理対象の特性

- マスタ145社のうち **MLITPermits に登録されている社のみ** が表示
- 「許可証必要社のみ管理」という運用方針 (Stage 1 で確認済)
- 新規許可証情報を追加する場合は MLITPermits シートに手動 INSERT、または GAS の検索機能で取得

## 将来追加メールが届いた場合の手順

新たに藤田さん経由で書類が届いた場合、以下のスクリプトを順次実行 (Stage 4 と同じフロー):

```bash
cd "C:/ProgramData/Generative AI/Github/construction-permit-tracker"

# 1. メール取り込み
python src/fetch_gmail.py --execute

# 2. ZIP 展開 (もし新規 ZIP がある場合)
# /tmp/stage4_extract.py を参考に実装、またはファイル毎に手動展開

# 3. 分類 dry-run
python scripts/classify_inbox.py
# → data/stage4_classification_<TS>.csv が生成される

# 4. CSV を Excel で開いて内容確認
#   - confidence HIGH/MEDIUM は default approved=Y
#   - UNMATCHED は手動でレビュー (社名特定不能)
#   - 必要なら approved 列を編集して保存

# 5. 取り込み execute
python scripts/import_classified_inbox.py --csv data/stage4_classification_<TS>.csv --execute

# 6. 一覧表 + 突合せ + ZIP 再生成
python scripts/generate_review_list_v2.py --execute
python scripts/generate_master_db_reconcile.py --mode aggressive --output-suffix v2
python scripts/build_delivery_folder.py --execute
cp output/review_list_v2_*.xlsx output/FDE_MANAGED/00_一覧表_v2_<TS>.xlsx
cp output/master_db_reconcile_v2_*.xlsx output/FDE_MANAGED/00_突合せレポート_v2_<TS>.xlsx
python scripts/package_delivery.py --package-id <TS>

# 7. GAS sync (rename 反映)
python scripts/sync_db_master_to_gas.py --execute
```

## ロールバック手順

Stage 1〜4 のロールバックは以下の順序で実施可能:

```bash
# Stage 4 ロールバック (DB INSERT + ファイル move を取り消し)
python scripts/import_classified_inbox.py --rollback-from data/inbox_move_plan_20260427_140154.json --execute

# Stage 4 DB ロールバック
cp data/permit_tracker.db.bak_pre_stage4_20260427_115756 data/permit_tracker.db

# Stage 3 ロールバック
cp data/permit_tracker.db.bak_pre_stage3_20260427_113953 data/permit_tracker.db
# (Stage 3 で move した originals は手動で archive から戻す、または stage3_state JSON 参照)

# Stage 2 ロールバック (DB rename + originals 統合)
cp data/permit_tracker.db.bak_pre_stage2_20260427_103414 data/permit_tracker.db
python scripts/cleanup_folders_to_master.py --rollback-from data/originals_move_plan_20260427_104730.json --execute

# Stage 1 ロールバック (read-only だったので不要、新規ファイル削除のみ)
rm output/review_list_v2_*.xlsx
rm output/master_db_reconcile_v2_*.xlsx
rm output/FDE_MANAGED/00_*_v2_*.xlsx
rm output/delivery_zips/FDE_MANAGED_20260427.zip
```

## バックアップファイル一覧

`data/` 配下に保管中。30 日保管後、安全確認の上で削除推奨:

```
permit_tracker.db.bak_pre_stage1_20260427_091419
permit_tracker.db.bak_pre_stage2_20260427_103414
permit_tracker.db.bak_pre_stage3_20260427_113953
permit_tracker.db.bak_pre_stage4_20260427_115756

originals_manifest_before_20260427_103414.json
originals_move_plan_20260427_104730.json
originals_verify_20260427_104730.json

inbox_manifest_before_stage4_20260427_115756.json
inbox_move_plan_20260427_140154.json

sheet_snapshots/sheet_snapshot_pre_stage2_20260427_104741.json
sheet_snapshots/sheet_snapshot_pre_stage2_20260427_161649.json

stage2_state_20260427_103414.json
```

## 残課題

### 短期 (次回着手候補)
1. ~~**C0089 フジ勢が ZIP に含まれない問題**~~ — **解決済 (2026-04-27)**: `build_delivery_folder.py` Line 178 の `cdir.glob("*.pdf")` を `cdir.rglob("*.pdf")` に変更。ZIP は 235→251 files、317.6MB。C0089 含む 88 社の最新版が ZIP に反映済。
2. **inbox_unclassified_20260427_140154/ 76 件** — ファイル名で社名特定不能だった分。藤田さん経由で送信元担当者リストを照会、または手動分類
3. **パスワード保護 ZIP 3 件** — 三晃金属工業株式会社 + 別 1 件、藤田さん経由でパスワード問い合わせ
4. **LZH 1 件** — `7zip` 等で手動展開、内容次第で取り込み判断

### 中期 (検討段階)
5. **Phase C 自動化** — daily fetch + dry-run メール通知の半自動パイプライン (ユーザー判断「もう少し検討」)
6. **GAS MLITPermits 拡充** — 許可証種類別タブ等 Webアプリ側の改善

### 長期 (再発防止)
7. **fetch_gmail.py の `original_sender` 自動推測ガード強化** — 過去 38 社自動登録暴走の再発防止 (`feedback_no_auto_company_creation.md`)
8. **マスタ更新フロー** — 藤田さんから新マスタ (例 150社版) を受領した場合の更新手順を確立

## 関連メモリ

- `memory/handoff.md` (Stage 1-4 経緯)
- `memory/project_master_alignment_20260427.md` (マスタ整合方針)
- `memory/project_data_reconciliation.md` (145社マスタ正本方針)
- `memory/feedback_no_auto_company_creation.md` (自動登録禁止ルール)
- `memory/reference_gas_urls.md` (GAS URL一覧)
