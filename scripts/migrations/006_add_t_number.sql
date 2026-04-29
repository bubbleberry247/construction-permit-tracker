-- migration 006: companies テーブルに t_number 列を追加
-- 基幹サーバの T番号マスタ (法人番号) と当方会社を紐付けるため

BEGIN TRANSACTION;

ALTER TABLE companies ADD COLUMN t_number TEXT;

CREATE INDEX IF NOT EXISTS idx_companies_t_number ON companies (t_number);

COMMIT;
