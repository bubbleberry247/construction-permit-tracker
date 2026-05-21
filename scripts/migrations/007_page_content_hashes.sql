-- migration 007: ページ画像ハッシュ管理テーブル
-- 同 sha256 で異 file_name の重複検出に使う (read-only レポート用)

BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS page_content_hashes (
  cid TEXT NOT NULL,
  file_name TEXT NOT NULL,
  page_no INTEGER NOT NULL,
  sha256 TEXT NOT NULL,
  hash_method TEXT NOT NULL,
  created_at TEXT DEFAULT (datetime('now','localtime')),
  PRIMARY KEY (cid, file_name, page_no)
);

CREATE INDEX IF NOT EXISTS idx_page_content_hashes_sha256
  ON page_content_hashes(sha256);

COMMIT;
