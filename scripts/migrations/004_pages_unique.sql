-- migration 004: pages テーブルに UNIQUE INDEX を追加（重複防止）
-- 前提: scripts/dedup_pages.py --execute で重複行を削除済み
-- (2026-04-29 commit 3dcbde1 で 1707 → 1664 行)

BEGIN TRANSACTION;

-- 同一 (company_id, file_name, page_no) は 1 行のみ
CREATE UNIQUE INDEX IF NOT EXISTS idx_pages_unique_company_file_page
  ON pages (company_id, file_name, page_no);

-- 検証クエリ（migration 適用後の手動確認用）:
--   SELECT * FROM sqlite_master WHERE type='index' AND name='idx_pages_unique_company_file_page';

COMMIT;
