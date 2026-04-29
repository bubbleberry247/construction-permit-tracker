# Workbook 分離設計 (GPT-5.5 Issue 6 対応)

GPT-5.5 レビュー指摘:
> 1091 decisions / 9 シートは藤田負担が大きく、放棄リスクが高い。
> 必須判断のみの Lite workbook と、社内全件確認の Full workbook に分離すべき。

## 設計

### Lite workbook (藤田送付用) — 必須判断のみ
**ファイル名**: `06_藤田向けLite_<TS>_<wf>.xlsx`

シート構成 (4 シート):

| シート | 内容 | 想定件数 |
|---|---|---|
| 0_workflow_meta | 機械保護メタ | - |
| 1_突合せ確認 | 表記揺れ未確定 (LOW/UNMATCHED) のみ | 3〜29 |
| 2_修正依頼確認 | 藤田自身の修正依頼 81 件のみ (項目展開せず元の 81 行のまま) | 81 |
| 3_期限管理 | 期限切れ + 期限90日以内のみ | 3 |

**藤田の作業時間目安**: 30分〜1時間

藤田が触らない項目 (社内処理):
- ページ別 doc_type 分類 (当方目視 → 必要なら別途相談)
- 会社基本情報の差分修正 (当方が藤田原本から取り込み)
- 既存対象外化候補 (シート 5 → 当方判断 + 藤田定期報告)
- 新規ファイル追加 (別途メール添付で受領)

### Full workbook (社内処理用) — 全件
**ファイル名**: `06_社内処理Full_<TS>_<wf>.xlsx` (現行 `06_担当者編集_v2_*.xlsx` を内部用に rename)

シート構成: 現行通り 9 シート (1091 decisions)

社内担当者 (Masaru) が処理:
- ページ再登録 (シート 7、8 時間目視作業)
- 会社基本情報マージ (シート 8、藤田データを取り込み)
- exemption 確定 (Phase R-C で抽出した 19 件 + 藤田指摘ベース)
- 業務判定確定 (機械算出 + 担当者調整)

### 連携フロー

```
1. Full workbook 生成 (社内、現行プロセス)
2. Lite workbook 生成 (藤田送付用、Full の subset)
3. 当方で Full の重い処理 (ページ再登録、exemption など) を進める
4. 藤田に Lite を送付 (3 つの依頼事項のみ)
5. 藤田から Lite 返送
6. apply_editor_decisions_v2.py で Lite + Full の両方を反映
   (Lite はインデックスとして workflow_id 突合せ)
7. 反映結果を v3 一覧表に統合
```

### 実装方針 (将来)

新規スクリプト `scripts/build_lite_workbook.py`:
- 入力: `06_担当者編集_v2_<TS>_<wf>.xlsx` (Full)
- 出力: `06_藤田向けLite_<TS>_<wf>.xlsx`
- 処理:
  1. Full の workflow_meta をコピー (workflow_id 共有 → apply 時に統合可能)
  2. シート 1 から LOW/UNMATCHED 行のみ抽出
  3. シート 2 を「修正依頼 81 件 (全件展開せず原文のまま)」に再構成
  4. シート 3 をそのままコピー
  5. シート 4〜9 は除外

apply_editor_decisions_v2.py 改修:
- 入力 wb の workflow_meta から「Lite/Full」判定 (フラグ追加)
- Lite モードでは シート 7, 8, 9 を読まない (skip)

## 採用判断

**現状**: 採用は `保留` (現行 Full workbook で進める)

**理由**:
- 現行 Full workbook は機械的に生成され、藤田が「触る部分は黄色だけ」と明示すれば運用可能
- Lite/Full 分離はスクリプト 2 本 + テスト工数が必要
- まず藤田に現行 Full を送り、反応を見てから判断 (重ければ Lite を後付け)

**判断基準**:
- 藤田が 1 週間以上返送しない → Lite 化検討
- 藤田から「重い、見るところが分からない」のフィードバック → Lite 化即実装
- 藤田が問題なく返送 → Lite 化不要

## 関連ファイル

- `scripts/build_editor_workbook_v2.py` (現行 Full)
- `scripts/apply_editor_decisions_v2.py` (反映、両モード対応予定)
- `~/.claude/plans/a-gpt5-5-lexical-pixel.md` (設計プラン)
