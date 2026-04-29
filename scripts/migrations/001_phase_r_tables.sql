-- Phase R 用 4 新規テーブル
-- 目的:
--   1. fujita_mapping: 藤田会社名 ↔ 当方 company_id の永続化 (再返送時に再利用)
--   2. companies_staging: 新規候補保留 (本体 companies に直接 INSERT 禁止)
--   3. decisions_log: 反映履歴 + 二重反映防止 (workflow_id で一意管理)
--   4. permit_expiry_status: 期限ステータス時系列

BEGIN TRANSACTION;

-- 1. 藤田名 ↔ company_id マッピング
CREATE TABLE IF NOT EXISTS fujita_mapping (
    mapping_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    fujita_row_id  INTEGER NOT NULL,           -- 継続取引業者リスト の row 番号
    fujita_name    TEXT NOT NULL,
    company_id     TEXT REFERENCES companies(company_id),  -- NULL = UNMATCHED/staging
    confidence     TEXT NOT NULL,              -- HIGH/MEDIUM/LOW/UNMATCHED
    match_method   TEXT,                       -- exact/normalized/manual
    confirmed_at   TEXT,                       -- 担当者承認日時 (NULL=未確認)
    confirmed_by   TEXT,
    fujita_file_ts TEXT NOT NULL,              -- 元ファイルのタイムスタンプ
    created_at     TEXT NOT NULL DEFAULT (datetime('now','localtime')),
    UNIQUE(fujita_row_id, fujita_file_ts)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_fujita_mapping_company ON fujita_mapping(company_id);
CREATE INDEX IF NOT EXISTS idx_fujita_mapping_name    ON fujita_mapping(fujita_name);

-- 2. 新規候補 staging (本体 companies に入れる前の保留)
CREATE TABLE IF NOT EXISTS companies_staging (
    staging_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    fujita_row_id  INTEGER,
    fujita_name    TEXT NOT NULL,
    proposed_company_id TEXT,                  -- 当方提案 (空欄なら昇格時採番)
    permit_number  TEXT,
    permit_authority TEXT,
    mlit_check_status TEXT DEFAULT 'pending', -- pending/confirmed/not_found/error
    mlit_check_at  TEXT,
    fujita_approval TEXT DEFAULT 'pending',   -- pending/approved/rejected
    our_approval   TEXT DEFAULT 'pending',    -- pending/approved/rejected
    promoted_at    TEXT,                       -- 本体 companies に INSERT 完了日時
    promoted_company_id TEXT,
    workflow_id    TEXT,                       -- 投入 workflow
    notes          TEXT,
    created_at     TEXT NOT NULL DEFAULT (datetime('now','localtime'))
) STRICT;

CREATE INDEX IF NOT EXISTS idx_staging_fujita ON companies_staging(fujita_row_id);
CREATE INDEX IF NOT EXISTS idx_staging_workflow ON companies_staging(workflow_id);

-- 3. 反映履歴 + 二重反映防止
CREATE TABLE IF NOT EXISTS decisions_log (
    decision_id    TEXT PRIMARY KEY,           -- UUID v4
    workflow_id    TEXT NOT NULL,
    company_id     TEXT,
    document_type  TEXT,
    decision_kind  TEXT NOT NULL,              -- doc_status/expiry_status/inactivate/staging_add
    old_value      TEXT,
    new_value      TEXT,
    decision_value TEXT NOT NULL,              -- 反映/保留/却下/INACTIVE化 等
    operator       TEXT,                        -- 担当者 (藤田/当方)
    fujita_comment TEXT,
    operator_comment TEXT,
    applied_at     TEXT NOT NULL DEFAULT (datetime('now','localtime')),
    source_workbook TEXT,                       -- 06_担当者編集_<TS>_<wf>.xlsx
    UNIQUE(workflow_id, decision_id)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_decisions_workflow ON decisions_log(workflow_id);
CREATE INDEX IF NOT EXISTS idx_decisions_company  ON decisions_log(company_id);

-- 4. 期限ステータス時系列
CREATE TABLE IF NOT EXISTS permit_expiry_status (
    status_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id     TEXT NOT NULL REFERENCES companies(company_id),
    permit_id      INTEGER REFERENCES permits(permit_id),
    attached_expiry TEXT,                       -- 添付許可証の期限
    mlit_expiry    TEXT,                        -- MLIT 公式期限
    expiry_status  TEXT NOT NULL,               -- ok/expiring_90/expiring_30/expired/renewed_confirmed
    next_action    TEXT,                        -- 督促/再依頼/MLIT再確認
    next_action_due TEXT,
    workflow_id    TEXT,
    notes          TEXT,
    recorded_at    TEXT NOT NULL DEFAULT (datetime('now','localtime'))
) STRICT;

CREATE INDEX IF NOT EXISTS idx_expiry_company ON permit_expiry_status(company_id);
CREATE INDEX IF NOT EXISTS idx_expiry_status  ON permit_expiry_status(expiry_status);

COMMIT;
