# Phase R 実行順序 (確定、GPT-5.5 Issue 4 反映)

藤田送付前の必須前処理を明確化。順序を守らないと藤田が見るデータと当方 DB がずれる。

## 推奨順序

### Step 1: Phase R-A (マスタ差分究明、read-only)
```bash
python scripts/master_diff_report.py "C:/Users/owner/Downloads/藤田さん作成　....xlsx"
```
出力: `output/FDE_MANAGED/09_master_diff_<TS>.xlsx`

ユーザーがシート 2 (藤田のみ 26 社) をレビュー、判定確定。

### Step 2: Phase R-Backup-2 (snapshot + git tag)
```bash
python scripts/snapshot_backup.py
git tag phase_r_pre_<TS>
```

### Step 3: Phase R-B Stage (新規候補を staging へ)
```bash
python scripts/add_companies_from_fujita.py output/FDE_MANAGED/09_master_diff_<TS>.xlsx --execute
```
SQL で staging を承認:
```sql
UPDATE company_additions_staging SET approval_status='approved',
       approved_by='masaru', approved_at=datetime('now','localtime')
WHERE staging_id IN (...);
```

### Step 4: Phase R-B Promote (staging → 本体 companies)
```bash
python scripts/add_companies_from_fujita.py --promote --execute
```
本体 companies に C0152〜 採番 INSERT。

### Step 5: Phase R-C-1 (修正依頼分類)
```bash
python scripts/extract_corrections_from_comments.py "C:/Users/owner/Downloads/藤田さん作成....xlsx"
```
出力: `output/FDE_MANAGED/10_corrections_classified_<TS>.csv`

### Step 6: Phase R-C-3 (exemption 自動登録) ← **重要、GPT-5.5 Issue 5**
```bash
python scripts/register_exemptions.py --execute
```
既知 exemption + Phase R-C で抽出した EXEMPTION を `company_doc_exemptions` に INSERT。
**この登録なしで wb 生成すると、藤田に既知論点を再確認させる。必ず先に実行。**

### Step 7: Phase R-C-2 (ページ再登録、当方目視 8h) ← **可能なら実施**
当方バンドル PDF を開いて藤田指摘ページに doc_type 付与。
- corrections_classified CSV の PAGE_REREGISTER 45 件をレビュー
- pages テーブルに INSERT (`page_doc_type_history` も同時に記録)

このステップを skip しても次に進めるが、藤田に「自分の指摘がまだ反映されていない」と思われる。

### Step 8: Phase R-D (業務判定再算出)
```bash
python scripts/classify_business_status.py
```
exemption + 再登録 page を反映した業務判定 CSV を再生成。

### Step 9: Phase R-E (編集 wb v2 生成)
```bash
python scripts/build_editor_workbook_v2.py --business-status-csv data/business_status_<TS>.csv
```
出力: `output/FDE_MANAGED/06_担当者編集_v2_<TS>_<wf>.xlsx`
+ `output/FDE_MANAGED/phase_re_auto_prefill_company_info_<TS>.csv` (当方取り込み用)

### Step 10: Lite workbook 生成 (オプション、藤田向け軽量版)
```bash
python scripts/build_lite_workbook.py output/FDE_MANAGED/06_担当者編集_v2_<TS>_<wf>.xlsx
```
出力: `output/FDE_MANAGED/06_藤田向けLite_<TS>_<wf>.xlsx` (4 シート、必須判断のみ)

### Step 11: 藤田送付 + 返信下書き調整
```bash
# docs/reply_to_fujita_20260429_v2.md を編集して送付
```

### Step 12: 藤田返送後の反映
```bash
python scripts/apply_editor_decisions_v2.py <返送 xlsx>            # dry-run
python scripts/apply_editor_decisions_v2.py <返送 xlsx> --execute  # 実行
```

---

## 順序を守らないとどうなるか

| Skip した step | 影響 |
|---|---|
| Step 1〜2 | rollback 不能、DB 破壊リスク |
| Step 3〜4 (R-B) | Sheet 4 が空、藤田に「新規追加した社」を見せられない、後で追加すると藤田の Sheet と DB がずれる |
| Step 6 (R-C-3 exemption) | Sheet 6/2 で既知 exemption が「書類不備」と表示、藤田に再説明させる |
| Step 7 (R-C-2 ページ再登録) | 藤田の修正依頼が反映されないまま wb 送付、藤田は「自分の前回指摘が無視された」と思う |
| Step 8 (R-D 再算出) | 業務判定が古いまま、Sheet 3/6 が誤った数字を表示 |

## Phase R の冪等性

- Step 1〜10 は何度実行しても OK (snapshot 復元 + 再実行可能)
- Step 12 の apply は workflow_id 単位で二重反映防止 (hash 検証付き)
- 各 step の出力ファイルは TS で区別、過去版を保持
