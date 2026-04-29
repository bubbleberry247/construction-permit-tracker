# 藤田さん宛 返信下書き v2 (Phase R 全 Phase 完結後)

## 件名 (継承)

`Re: 新規・継続取引契約書につきまして`

## 本文

```
藤田 様

お忙しい中ご集計いただきありがとうございました。
当方で件数差を解析したところ、判定軸が異なっていたことが主因と分かりました。
当方は書類8点の機械的な揃い (◎/△/×) で集計しており、藤田様側は
業務上の有効性 (有効/書類不備/未提出/期限/対象外) を判定いただいて
いると理解しました。これは別軸の集計のため、両方を残した編集シートに
整え直しました。

ご依頼事項 (3 点)

1. 突合せ確認
   会社名の表記ゆれで突合せ未確定の社が一定数ございました
   (例: Ｇテリア / Gテリア など)。編集シート「1_突合せ確認」で
   「同一/別社/新規候補/保留」のご選択をお願いいたします。

2. 反映可否選択
   「修正依頼」欄にいただいたコメントを当方で項目ごとに展開しました。
   編集シート「2_受領状況編集」で「反映/保留/却下」をご選択いただき、
   「反映」の場合は反映後の値 (○/×) もご指定ください。

3. 対象外・追加候補確認
   藤田様マスタのみ記載の社、対象外と判定された社につきまして、
   今後の管理方針 (新規追加/INACTIVE化/別名疑い等) をご指示ください。
   編集シート「4_新規追加済確認」「5_既存対象外化候補」で選択可能です。

なお、株式会社三富 (期限切れ判定) については、当方で MLIT 公式を確認した
ところ許可は更新済 (新期限 2031-04-06) でした。添付の許可証は更新前の
ものですので、備考欄に注記しております。

【ご参考】編集シートには以下の機能を追加しております:
- 「7_ページ別分類確認」: 当方が判定した PDF のページ別書類分類をご確認・
   修正できます (会社概要に記載の取引先一覧などはこのシートで「正/誤/別の
   書類」として修正可能)
- 「8_会社基本情報修正」: 代表者名・連絡先などの修正もできます
- 「9_新規受領ファイル追加」: 藤田様にお手元にある PDF があれば、
   会社ID と書類カテゴリをご記入いただければ当方で取り込みます

編集シート (06_担当者編集_v2_*.xlsx) と使い方 (Markdown/PDF) を本メールに
添付いたします。お手数をおかけしますが、編集後にお戻しいただき次第、
当方で v3 一覧表と DB に反映いたします。

よろしくお願い申し上げます。
```

## 添付ファイル

1. `output/FDE_MANAGED/06_担当者編集_v2_20260429_phaseRE_fa33be2f.xlsx` (編集シート、9 シート / 1091 decisions pre-filled)
2. `docs/editor_workflow_for_fujita.md` (使い方ガイド、PDF 化推奨)
3. `output/FDE_MANAGED/09_master_diff_20260429_105905.xlsx` (マスタ差分レポート、参考)

## 送信前チェックリスト

- [ ] 編集シートの workflow_id がファイル名と一致 (`fa33be2f`)
- [ ] シート 0 (workflow_meta) が編集禁止になっている
- [ ] ドロップダウンが効く (シート 2 の任意セルで Alt+↓)
- [ ] CC に必要な人を追加
- [ ] 元スレッドへの「返信」になっている

## 補足: 当方処理フロー (社内向け)

藤田からの返送後:
```bash
# dry-run (内容確認)
python scripts/apply_editor_decisions_v2.py <返送 xlsx>

# execute (反映)
python scripts/apply_editor_decisions_v2.py <返送 xlsx> --execute

# v3 一覧表生成 (Phase R-D 業務判定 + exemption 反映)
python scripts/classify_business_status.py
# → 必要に応じて v3 ハイパーリンク + ZIP 再生成 + GAS sync
```

問題があれば snapshot 復元:
```bash
python scripts/apply_editor_decisions_v2.py --rollback 20260429_pre_master_diff
```

## 進捗状況 (社内向け)

Phase R 全 Phase 構築完了 (2026-04-29):
- Phase R-A: マスタ差分レポート生成済 (output/FDE_MANAGED/09_master_diff_*.xlsx)
- Phase R-Backup-2: snapshot + git tag 取得済 (TS=20260429_pre_master_diff)
- Phase R-B: 25 社追加スクリプト作成済 (dry-run 済、--execute 待ち)
- Phase R-C: 修正依頼分類済 (PAGE_REREGISTER 45 / EXEMPTION 19 / OTHER 33)
- Phase R-D: 業務判定エンジン (機械化、当方算出: 有効38/書類不備41/未提出38/期限切れ2/期限90日以内1)
- Phase R-E: 編集 wb v2 生成済 (9 シート、1091 decisions pre-filled)
- Phase R-F: 反映スクリプト v2 作成済 (dry-run 動作確認済)

次のアクション:
1. ユーザー (Masaru) が `09_master_diff_*.xlsx` シート 2 を確認 (25 社の追加判定)
2. 確定後 `python scripts/add_companies_from_fujita.py ... --execute` で 25 社追加
3. ページ再登録 (Phase R-C 当方目視作業 8 時間程度)
4. 編集 wb 再生成 → 藤田送付
