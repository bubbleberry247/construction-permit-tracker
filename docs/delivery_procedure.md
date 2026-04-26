# 添付書類一式の納品手順（藤田さん向け）

## 全体フロー

```
[1] フォルダ構築 → [2] 一覧表同梱 → [3] ZIP化 → [4] Drive アップ → [5] 共有リンク取得 → [6] メール返信
```

所要: 30〜60分（うち Drive アップロードが大半）

---

## 前提

- ローカルに最新の DB と originals があること
- Google Drive の容量に 350MB 以上の余裕があること
- 藤田さんとの過去メールスレッド（返信先）が手元にあること

## 手順

### 1. 最新一覧表を生成

```cmd
cd /d "c:\ProgramData\Generative AI\Github\construction-permit-tracker"
python scripts\generate_review_list.py
```

→ `output\review_list_YYYYMMDD_HHMMSS.xlsx` が出力される

### 2. フォルダ構造を構築

```cmd
python scripts\build_delivery_folder.py --execute
```

出力: `output\FDE_MANAGED\10_originals\C0XXX_<会社名>\<書類カテゴリ>\<受領年>\<原本.pdf>`

### 3. 一覧表をフォルダに同梱

```cmd
copy "output\review_list_YYYYMMDD_HHMMSS.xlsx" "output\FDE_MANAGED\00_一覧表_YYYYMMDD.xlsx"
```

### 4. ZIP にパッケージング

```cmd
python scripts\package_delivery.py
```

→ `output\delivery_zips\FDE_MANAGED_YYYYMMDD.zip`（README 同梱）

### 5. Google Drive にアップロード

ブラウザで手動アップロード（サービスアカウント経由は所有権の問題があるため）:

1. https://drive.google.com を開く
2. 適切なフォルダ（例: `My Drive/協力会社書類_納品/`）に移動
3. 上部「+ 新規」→「ファイルのアップロード」
4. `output\delivery_zips\FDE_MANAGED_YYYYMMDD.zip` を選択
5. アップロード完了まで待つ（数分）

### 6. 共有リンクを取得して藤田さんに共有

1. アップした ZIP を右クリック → 「共有」
2. 「リンクを知っているユーザー全員」または「藤田さんメールアドレス」を追加
3. 権限: **閲覧者**
4. 「リンクをコピー」

### 7. 藤田さんに返信メール

藤田さんからの元メールを Gmail で開いて「返信」、下書き本文（[`delivery_email_template.md`](delivery_email_template.md) 参照）を貼り付け、共有リンクを差し込んで送信。

---

## 確認ポイント

- **ZIP サイズ**: 約 314 MB（91 社分、230 PDF + README + 一覧表）
- **対象会社数**: 142 社（うち届いてる 91 社のみフォルダあり、残り 51 社は空）
- **manifest_sha256.txt**: 全ファイルの SHA-256 ハッシュ。改ざん検知用に同梱

## トラブルシューティング

| 症状 | 対応 |
|---|---|
| build_delivery_folder.py で `PDF なし会社` が増えた | originals フォルダに PDF が落ちてないか確認 |
| ZIP サイズが想定と大きく違う | 受領 PDF が増減した可能性。`output/FDE_MANAGED/` を一度削除して再ビルド |
| Drive アップロードが失敗 | ブラウザを更新、または小さいフォルダに分けてアップ |
| 一覧表に不足あり社が含まれる | `△一部受信` 32 社の対応状況を別途連絡 |

## 月次運用フロー

毎月新規受領があれば:
1. メール処理（Gmail 受信箱から添付 PDF を取り込み）
2. OCR + 分類（既存パイプライン）
3. このドキュメントの手順 1〜7 を実行
4. 藤田さんに「○月分追加」として送付
