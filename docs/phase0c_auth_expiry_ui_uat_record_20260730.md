# Phase 0-C 認証期限切れUI修正 UAT記録

## 1. 概要

- 実施日: 2026-07-30 JST
- 対象: 建設業許可証管理システム UAT
- 本番反映: 未実施
- 外部メール送信: 0件
- notification mode変更: なし
- MLIT mode変更: なし
- トリガー変更: なし

会社マスタ画面の表本体に
`Google認証claimが無効です`と表示され、会社一覧が消えたように見える事象を
調査した。会社データや一覧機能は消失しておらず、ブラウザメモリに保持していた
Google ID tokenの期限切れを、通常の一覧取得エラーとして描画していたことが
直接原因だった。

上流原因は、認証期限切れ処理がAPI共通入口になく、画面ごとのcatchへ
流れていたことである。

## 2. 修正内容

- API共通入口で`UNAUTHORIZED`を検知する。
- token、利用者、編集中会社、通知プレビュー等のクライアント状態を消去する。
- 開いているdialogを閉じる。
- アプリ本体を非表示にし、Google再ログイン画面へ戻す。
- 会社一覧とMLIT一覧では、認証エラーを表のエラー行として描画しない。
- その他の画面でも、認証エラーを通常toastとして重ねて表示しない。
- 入力エラー、同時更新、権限拒否等の非認証エラー表示は維持する。
- 再ログイン成功後は、直前の管理画面ではなく会社マスタを先頭表示する。

## 3. テストと独立レビュー

| suite | 結果 |
|---|---:|
| Python回帰 | 415/415 PASS |
| Phase 0送信安全 | 31/31 PASS |
| 認証・会社・通知・移行 | 39/39 PASS |
| MLIT同期・期限差分 | 13/13 PASS |
| 運用統制 | 25 assertions PASS |
| 総チェック数 | 523/523 PASS |

独立レビューは初回に、APIの再throw後も個別catchがエラーを描画する経路を
指摘した。全authenticated catchへ認証エラー抑止を反映後、
再レビューで`APPROVE`となった。

## 4. UAT反映

| 項目 | 値 |
|---|---|
| Spreadsheet ID | `1Hzr72GZgxtLRr1bL_SUqPSjoKEDx521b6M1IxV_y-EQ` |
| Apps Script ID | `1urr5VGVUkKXO6xV_SrSQwT3l4R1hfPH76dSdReTxp_QsszzK_5PwaMFH` |
| 固定deployment | `AKfycbwYyjjQw4ZYag37JSOexxGJHFwZG78a69-Dcmi2DcT7XoHoOAQyGTCrR5EgWbpVMm9Y2w @9` |
| 反映commit | `2aa012b` |
| Apps Script file | 27件 |

固定Web URL:

`https://script.google.com/macros/s/AKfycbwYyjjQw4ZYag37JSOexxGJHFwZG78a69-Dcmi2DcT7XoHoOAQyGTCrR5EgWbpVMm9Y2w/exec`

`clasp deployments`で固定deploymentが`@9`を指すことを確認した。

## 5. 実ブラウザ確認

確認済み:

- v9固定URLが「建設業許可証管理システム」を表示する。
- 期限切れ状態を保持しない再読み込み後、Google再ログイン画面へ戻る。
- 一覧内に認証claimエラー行を残さない。

未確認:

- `kalimistk@gmail.com`で再ログイン後、会社マスタが先頭表示されること。
- 会社件数が131社であること。

Googleアカウント選択は利用者操作として実施し、その直後に上記2点を追記する。
