-- GPT-5.5 レビュー反映 (REQUEST_CHANGES 5 件) 用 migration
-- Issue 1: company_additions_staging (Phase R-B 直接 INSERT 廃止 → staging 経由)
-- Issue 3: decisions_log に source_row_hash + edited_value_hash 列追加
-- Issue 5: page_doc_type_history (pages 分類変更履歴)

BEGIN TRANSACTION;

-- Issue 5: pages 分類変更履歴
CREATE TABLE IF NOT EXISTS page_doc_type_history (
    history_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    page_id         INTEGER NOT NULL REFERENCES pages(page_id),
    company_id      TEXT NOT NULL,
    file_name       TEXT,
    page_no         INTEGER,
    old_doc_type_name TEXT,
    new_doc_type_name TEXT,
    reason          TEXT,
    workflow_id     TEXT,
    decision_id     TEXT,
    confirmed_by    TEXT,
    confirmed_at    TEXT NOT NULL DEFAULT (datetime('now','localtime'))
) STRICT;

CREATE INDEX IF NOT EXISTS idx_page_history_page    ON page_doc_type_history(page_id);
CREATE INDEX IF NOT EXISTS idx_page_history_company ON page_doc_type_history(company_id);
CREATE INDEX IF NOT EXISTS idx_page_history_workflow ON page_doc_type_history(workflow_id);

-- Issue 3: decisions_log に hash 列追加
-- (SQLite は ALTER TABLE で列追加可能、UNIQUE 制約は事後追加不可なので注意)
ALTER TABLE decisions_log ADD COLUMN source_row_hash TEXT;
ALTER TABLE decisions_log ADD COLUMN edited_value_hash TEXT;

-- Issue 1: company_additions_staging (新規会社追加の staging)
CREATE TABLE IF NOT EXISTS company_additions_staging (
    staging_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    fujita_row_id    INTEGER,
    fujita_name      TEXT NOT NULL,
    proposed_company_id TEXT,                 -- 採番後 (NULL なら未採番)
    permit_number    TEXT,
    permit_authority TEXT,
    fujita_status    TEXT,                     -- 元の藤田 status
    user_judgment    TEXT,                     -- 「追加」「対象外」等
    source_row_hash  TEXT NOT NULL,            -- 入力 xlsx の行 hash
    source_file_name TEXT,
    workflow_id      TEXT,
    approval_status  TEXT DEFAULT 'pending',  -- pending/approved/rejected/promoted
    approved_by      TEXT,
    approved_at      TEXT,
    promoted_at      TEXT,                     -- companies に INSERT 完了
    promoted_company_id TEXT,
    notes            TEXT,
    created_at       TEXT NOT NULL DEFAULT (datetime('now','localtime')),
    UNIQUE(source_row_hash, workflow_id)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_add_staging_workflow ON company_additions_staging(workflow_id);
CREATE INDEX IF NOT EXISTS idx_add_staging_approval ON company_additions_staging(approval_status);

COMMIT;
