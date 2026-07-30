# Phase 0-C Google認証 UAT実施記録

作成日: 2026-07-30

対象branch: `codex/phase0-p1-safety`

対象環境: 本番とは別のApps Script／複製Google Sheets

判定: **技術管理者ログイン PASS／客先2アカウントの権限別UATはGoogle identity未準備のため未実施**

## 1. 結論

Apps Script HTML Service上でGoogle Identity Servicesを直接実行する旧案は、
実ブラウザでGoogleの`400: origin_mismatch`により失敗した。

根本原因は、画面本体がApps Scriptの固定URLではなく、実行ごとに生成される
Google所有の`googleusercontent.com` sandbox iframe originで動くことにある。
Google CloudはこのGoogle所有originをOAuth Web clientの
Authorized JavaScript originsとして登録できない。そのため、
JavaScript originの追加では解決できない構造上の問題だった。

認証方式をサーバー側のGoogle OIDC Authorization Code flow + PKCEへ変更し、
固定UAT deployment v8で次を確認した。

- Googleのアカウント選択画面が開き、`origin_mismatch`が発生しない。
- `kalimistk@gmail.com`でopenid／email／profileの同意を完了できる。
- OAuth callbackでauthorization codeをサーバー側交換できる。
- 返却されたID tokenをサーバーが検証し、`UserAccess`からroleを決定する。
- 元画面が一回限りのpollでtokenを受け取り、自動ログインする。
- `technical_admin`として131社の会社一覧を読める。
- ブラウザ申告のemail、role、送信権限を使用しない。

本番Apps Script、本番Google Sheets、本番deployment、本番公開範囲、
本番トリガー、外部メール送信には変更を加えていない。

## 2. 実装方式

### 公開入口

外部から呼べるApps Script関数は`doGet`と`apiDispatch`だけとし、
dispatcher内で匿名利用できるactionを次の2つだけ追加した。

- `auth.start`
- `auth.poll`

その他の会社、通知、MLIT、運用管理APIは、検証済みID tokenと
有効な`UserAccess`がなければ拒否する。

### 認証開始

`auth.start`は次を実行する。

1. Script Propertiesのclient ID、server-only client secret、固定redirect URIを検証。
2. 96文字のランダムstateとPKCE verifierを生成。
3. verifierのSHA-256からS256 code challengeを生成。
4. verifier、状態、poll回数をScript Cacheへ10分だけ保存。
5. ブラウザへstate、Google authorization URL、有効秒数だけを返す。

client secret、PKCE verifier、ID tokenはauthorization URLへ含めない。

### callbackとtoken受渡し

Googleから固定UAT `/exec`へ戻ったcallbackは次を実行する。

1. stateの形式とScript Cache上の一回限り状態を検証。
2. `PENDING`をScriptLock内で`EXCHANGING`へ変更。
3. authorization code、PKCE verifier、server-only client secretを使い、
   Google token endpointでID tokenへ交換。
4. ID tokenの`aud`、`iss`、`exp`、`email_verified`を検証。
5. emailを正規化し、有効な`UserAccess`のroleとcapabilityを取得。
6. 成功結果をScript Cacheへ3分だけ保存。
7. 元画面の`auth.poll`がID tokenを一度取得した時点でcacheを削除。

ID tokenはブラウザメモリだけに置き、`localStorage`、`sessionStorage`、
Google Sheets、Script Properties、AuditLogへ保存しない。

### fail-closed制御

- state再利用を拒否。
- state形式不正、期限切れ、未登録stateを拒否。
- callbackの二重処理を拒否。
- pollは最大120回。
- 認証開始は全体で1分30回まで。
- ScriptLockを5秒以内に取得できない場合はデータを変更しない。
- OAuth provider障害、token交換失敗、UserAccess未登録・inactiveを拒否。
- providerのerror description、authorization code、token、秘密情報を監査へ残さない。
- callback画面の文字列をHTML escapeする。

## 3. Google Cloud設定

Google Cloud project `permit-tracker-tic`で次をUAT専用に設定した。

- Google Auth Platform app:
  `建設業許可証管理システム UAT`
- audience: External／Testing
- OAuth client type: Web application
- OAuth client:
  `建設業許可証管理システム UAT Web`
- Authorized JavaScript origins: なし
- Authorized redirect URI:
  固定UAT deploymentの`/exec` URL 1件だけ

Script Propertiesへ次を保存した。

- `GOOGLE_CLIENT_ID`
- `GOOGLE_CLIENT_SECRET`
- `GOOGLE_OAUTH_REDIRECT_URI`

秘密値はリポジトリ、文書、Google Sheets、画面証跡へ転記していない。

## 4. UAT deployment

| 項目 | 値 |
|---|---|
| Spreadsheet ID | `1Hzr72GZgxtLRr1bL_SUqPSjoKEDx521b6M1IxV_y-EQ` |
| Apps Script ID | `1urr5VGVUkKXO6xV_SrSQwT3l4R1hfPH76dSdReTxp_QsszzK_5PwaMFH` |
| 固定deployment | `AKfycbwYyjjQw4ZYag37JSOexxGJHFwZG78a69-Dcmi2DcT7XoHoOAQyGTCrR5EgWbpVMm9Y2w @8` |
| 反映commit | `8e65c17` |
| Apps Script file | 27件 |
| 実行主体 | deployment所有者 |
| Web endpoint | 到達可能。業務APIはGoogle OIDC + UserAccess必須 |

固定Web URL:

`https://script.google.com/macros/s/AKfycbwYyjjQw4ZYag37JSOexxGJHFwZG78a69-Dcmi2DcT7XoHoOAQyGTCrR5EgWbpVMm9Y2w/exec`

`clasp deployments`で上記固定deploymentが`@8`を指すことを確認した。

## 5. 実ブラウザUAT

実施日時: 2026-07-30 17時台 JST

### 技術管理者

| 確認項目 | 結果 |
|---|---|
| アカウント | `kalimistk@gmail.com` |
| OAuth account chooser | PASS |
| openid／email／profile同意 | PASS |
| callback code交換 | PASS |
| 元画面の自動ログイン | PASS |
| サーバー決定role | `technical_admin` |
| 外部送信権限 | なし |
| 会社一覧 | 131社 |

### ログイン直後の運用状態

| 項目 | 実値 |
|---|---:|
| notification mode | `OFF` |
| 送信準備完了会社 | 0 |
| 要設定会社 | 131 |
| 残り受信者quota | 100 |
| MLIT mode | `OFF` |
| MLIT最終成功から7日超 | 18 |
| MLIT確認待ち差分 | 0 |
| 送信元 | 未設定・不一致 |
| managed trigger | 0 |

したがって、トリガーの有無だけに依存せず、
notification mode、会社連絡先、送信元照合、送信権限、中央送信ゲートの
複数条件で外部送信を停止している。

## 6. テスト

v8反映前の最終回帰:

| suite | 結果 |
|---|---:|
| Python回帰 | 415/415 PASS |
| Phase 0送信安全 | 31/31 PASS |
| 認証・会社・通知・移行 | 38/38 PASS |
| MLIT同期・期限差分 | 13/13 PASS |
| 運用統制 | PASS |
| 数値化できるテスト合計 | 497/497 PASS |

OAuth追加回帰には、設定不足、匿名action限定、state再利用、
期限切れ、poll上限、lock失敗、provider障害、秘密情報非露出を含む。

## 7. 残ゲート

Google Auth Platformのtest userに追加できたのは
`kalimistk@gmail.com`だけだった。

次の2つはGoogle Cloud画面で
「Googleアカウント、Google Workspaceアカウント、Cloud Identityアカウントに
関連付けられていない」として追加を拒否された。

- `m-fujita@tokai-ic.co.jp`
- `kanri.tic@tokai-ic.co.jp`

これはアプリのrole実装ではなく、UATに使うGoogle identityの準備不足である。
本番公開へ進む前に、次のどちらかを正式に決める。

1. 上記メールをGoogle WorkspaceまたはCloud Identityのアカウントとして用意する。
2. 客先担当者が実際に使用する別のGoogleアカウントを指定し、
   `UserAccess`とOAuth test userを同じメールへ更新する。

その後、`master_editor`と`operations_admin`で検索、連絡先更新、同時更新、
権限拒否、MLIT確認、運用管理の権限別UATを行う。

技術管理者1アカウントのPASSだけでは3アカウントUAT完了、
客先公開可、本番反映可とは判定しない。
