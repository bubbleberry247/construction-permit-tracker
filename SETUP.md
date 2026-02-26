# clasp セットアップ手順

## 初回セットアップ

### 1. clasp インストール
```bash
npm install
```

### 2. Google アカウントでログイン
```bash
npx clasp login
```

### 3. GAS プロジェクトを作成 or 既存に接続

**新規作成（スプレッドシートにバインド）**:
```bash
# スプレッドシートを先に作成し、スクリプトエディタを開いてスクリプトIDを確認
# スクリプトID は URL: https://script.google.com/home/projects/{scriptId}
```

**.clasp.json を作成**:
```bash
cp .clasp.json.template .clasp.json
# .clasp.json の YOUR_SCRIPT_ID_HERE を実際のスクリプトIDに書き換える
```

### 4. コードをプッシュ
```bash
npm run push
```

### 5. スクリプトエディタで確認・実行
```bash
npm run open
```

## 開発フロー

| コマンド | 説明 |
|---|---|
| `npm run push` | ローカル → GAS に反映 |
| `npm run pull` | GAS → ローカルに取得 |
| `npm run open` | ブラウザでスクリプトエディタを開く |

## 初期化手順（GAS側）

1. スクリプトエディタで `initSheetHeaders()` を手動実行
2. Config シートに設定値を入力（README.md 参照）
3. `checkConfig()` で設定確認
4. `sendTestEmail('あなたのメール')` でメール送信テスト
5. `ENABLE_SEND=true` に変更して本番稼働
