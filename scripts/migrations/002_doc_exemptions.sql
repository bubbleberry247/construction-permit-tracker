-- Phase R-C 用: 会社別 不要書類テーブル
-- 目的:
--   会社単位で「この書類は不要 / 代替資料で OK / 一時免除」を機械記録
--   業務判定 (有効/書類不備) を機械化するための基礎データ

BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS company_doc_exemptions (
    exemption_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id      TEXT NOT NULL REFERENCES companies(company_id),
    document_type   TEXT NOT NULL,
    exempt_kind     TEXT NOT NULL,    -- NOT_APPLICABLE / ALTERNATIVE / TEMPORARY_WAIVER / REJECTED
    reason          TEXT,
    source_workflow TEXT,             -- どの workflow_id で確定したか
    confirmed_by    TEXT,             -- fujita / fde_team / auto_proposal
    confirmed_at    TEXT NOT NULL DEFAULT (datetime('now','localtime')),
    UNIQUE(company_id, document_type)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_doc_exemptions_company ON company_doc_exemptions(company_id);
CREATE INDEX IF NOT EXISTS idx_doc_exemptions_kind    ON company_doc_exemptions(exempt_kind);

COMMIT;
