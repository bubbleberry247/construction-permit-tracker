-- migration 005: pages テーブルに doc_type_secondary 列を追加
-- 1 ページが複数の役割を持つケース（例: 会社案内 PDF 内に取引先一覧表のページがある）に対応
--
-- 設計: 主分類 = doc_type_name、副分類 = doc_type_secondary（NULL 可）
-- 一覧表生成時は両方を OR で取り扱う
-- 主分類は引き続き必須（NULL 不可ではないが運用上ほぼ常に値あり）

BEGIN TRANSACTION;

ALTER TABLE pages ADD COLUMN doc_type_secondary TEXT;

COMMIT;

-- 適用後検証:
--   SELECT name, sql FROM sqlite_master WHERE type='table' AND name='pages';
