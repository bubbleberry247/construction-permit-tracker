-- Migration 009: pages 表に quarantine 関連列を追加 (GPT-5.5 推奨設計)
--
-- 物理削除を避けて非アクティブ化することで、ロールバック・監査を可能にする。
-- 集計クエリは WHERE is_active=1 でフィルタする。

ALTER TABLE pages ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1;
ALTER TABLE pages ADD COLUMN quarantine_reason TEXT;
ALTER TABLE pages ADD COLUMN quarantine_group_id TEXT;
ALTER TABLE pages ADD COLUMN canonical_page_id INTEGER;
ALTER TABLE pages ADD COLUMN dedup_run_id TEXT;
ALTER TABLE pages ADD COLUMN quarantined_at TEXT;

CREATE INDEX IF NOT EXISTS idx_pages_is_active ON pages(is_active);
CREATE INDEX IF NOT EXISTS idx_pages_dedup_run ON pages(dedup_run_id);
CREATE INDEX IF NOT EXISTS idx_pages_canonical ON pages(canonical_page_id);
