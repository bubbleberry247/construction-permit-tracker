"""
建設業許可証管理DB -- SQLite正本

使用法:
    from db import get_connection, init_db
    init_db()  # 初回のみ
    conn = get_connection()
"""
import sqlite3
import sys
from pathlib import Path
from typing import Optional

sys.stdout.reconfigure(encoding="utf-8")

DB_PATH = Path(__file__).parent.parent / "data" / "permit_tracker.db"

# ---------------------------------------------------------------------------
# 接続ヘルパー
# ---------------------------------------------------------------------------

def get_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """PRAGMA設定済みの接続を返す"""
    p = db_path or DB_PATH
    conn = sqlite3.connect(str(p))
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

_DDL = """
-- ===== 企業 =====
CREATE TABLE IF NOT EXISTS companies (
    company_id   TEXT PRIMARY KEY,
    official_name TEXT NOT NULL,
    name_aliases  TEXT,            -- pipe区切り
    corporation_type TEXT,         -- CORPORATION / SOLE_PROPRIETOR
    permit_number TEXT,
    permit_authority TEXT,
    mlit_status   TEXT DEFAULT 'NOT_CONFIRMED',
    last_confirmed_at TEXT,
    status        TEXT DEFAULT 'ACTIVE',
    created_at    TEXT NOT NULL DEFAULT (datetime('now','localtime')),
    updated_at    TEXT NOT NULL DEFAULT (datetime('now','localtime'))
) STRICT;

-- ===== 企業メール =====
CREATE TABLE IF NOT EXISTS company_emails (
    email_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id   TEXT NOT NULL REFERENCES companies(company_id),
    email        TEXT NOT NULL,
    email_type   TEXT NOT NULL DEFAULT 'sender',   -- sender / recipient
    created_at   TEXT NOT NULL DEFAULT (datetime('now','localtime'))
) STRICT;

CREATE UNIQUE INDEX IF NOT EXISTS uq_company_email
    ON company_emails(company_id, email, email_type);

-- ===== 受信メール =====
CREATE TABLE IF NOT EXISTS inbound_messages (
    message_id      TEXT PRIMARY KEY,
    company_id      TEXT REFERENCES companies(company_id),
    sender_email    TEXT,
    original_sender TEXT,
    received_at     TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now','localtime'))
) STRICT;

-- ===== ファイル =====
CREATE TABLE IF NOT EXISTS files (
    file_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id    TEXT REFERENCES inbound_messages(message_id),
    company_id    TEXT REFERENCES companies(company_id),
    file_name     TEXT NOT NULL,
    file_hash     TEXT,
    file_size_bytes INTEGER,
    saved_path    TEXT,
    new_filename  TEXT,
    new_path      TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now','localtime'))
) STRICT;

-- ===== ページ =====
CREATE TABLE IF NOT EXISTS pages (
    page_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id       INTEGER REFERENCES files(file_id),
    company_id    TEXT REFERENCES companies(company_id),
    file_name     TEXT,
    file_hash     TEXT,
    page_no       INTEGER NOT NULL,
    doc_type_id   INTEGER,
    doc_type_name TEXT,
    confidence    REAL,
    rotation      INTEGER DEFAULT 0,
    created_at    TEXT NOT NULL DEFAULT (datetime('now','localtime'))
) STRICT;

-- ===== OCR実行 =====
CREATE TABLE IF NOT EXISTS ocr_runs (
    run_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    page_id       INTEGER REFERENCES pages(page_id),
    run_type      TEXT NOT NULL DEFAULT 'initial',   -- initial / reocr
    model_name    TEXT,
    started_at    TEXT,
    finished_at   TEXT,
    status        TEXT DEFAULT 'completed',
    raw_response  TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now','localtime'))
) STRICT;

-- ===== OCRフィールド =====
CREATE TABLE IF NOT EXISTS ocr_fields (
    field_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id        INTEGER NOT NULL REFERENCES ocr_runs(run_id),
    field_name    TEXT NOT NULL,
    raw_value     TEXT,
    normalized    TEXT,
    confidence    REAL,
    created_at    TEXT NOT NULL DEFAULT (datetime('now','localtime'))
) STRICT;

-- ===== フィールドレビュー =====
CREATE TABLE IF NOT EXISTS field_reviews (
    review_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    field_id      INTEGER REFERENCES ocr_fields(field_id),
    company_id    TEXT REFERENCES companies(company_id),
    field_name    TEXT NOT NULL,
    confirmed_value TEXT,
    confirmed_by  TEXT NOT NULL DEFAULT 'user_manual',
    reviewed_at   TEXT NOT NULL DEFAULT (datetime('now','localtime')),
    created_at    TEXT NOT NULL DEFAULT (datetime('now','localtime'))
) STRICT;

-- ===== 許可証 =====
CREATE TABLE IF NOT EXISTS permits (
    permit_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id    TEXT NOT NULL REFERENCES companies(company_id),
    permit_number TEXT,
    permit_authority TEXT,
    permit_category TEXT,             -- 般 / 特
    permit_year   TEXT,
    issue_date    TEXT,
    expiry_date   TEXT,
    current_flag  INTEGER NOT NULL DEFAULT 1,
    source        TEXT DEFAULT 'ocr', -- ocr / manual / reocr
    created_at    TEXT NOT NULL DEFAULT (datetime('now','localtime')),
    updated_at    TEXT NOT NULL DEFAULT (datetime('now','localtime'))
) STRICT;

-- ===== 許可業種 =====
CREATE TABLE IF NOT EXISTS permit_trades (
    trade_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    permit_id     INTEGER NOT NULL REFERENCES permits(permit_id),
    trade_name    TEXT NOT NULL,
    created_at    TEXT NOT NULL DEFAULT (datetime('now','localtime'))
) STRICT;

-- ===== ジョブ =====
CREATE TABLE IF NOT EXISTS jobs (
    job_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    job_type      TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending',
    started_at    TEXT,
    finished_at   TEXT,
    result_summary TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now','localtime'))
) STRICT;

-- ===== エクスポート実行 =====
CREATE TABLE IF NOT EXISTS export_runs (
    export_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id        INTEGER REFERENCES jobs(job_id),
    export_type   TEXT NOT NULL,
    file_path     TEXT,
    row_count     INTEGER,
    created_at    TEXT NOT NULL DEFAULT (datetime('now','localtime'))
) STRICT;
"""

# ---------------------------------------------------------------------------
# ビュー
# ---------------------------------------------------------------------------

_VIEWS = """
-- v_current_fields: confirmedがあればそれ、なければ最新normalized
CREATE VIEW IF NOT EXISTS v_current_fields AS
SELECT
    of.field_id,
    of.run_id,
    of.field_name,
    of.raw_value,
    of.normalized,
    fr.confirmed_value,
    COALESCE(fr.confirmed_value, of.normalized, of.raw_value) AS effective_value,
    fr.confirmed_by,
    oruns.page_id,
    oruns.run_type
FROM ocr_fields of
JOIN ocr_runs oruns ON oruns.run_id = of.run_id
LEFT JOIN field_reviews fr ON fr.field_id = of.field_id
;

-- v_current_permits: 現在有効な許可証一覧
CREATE VIEW IF NOT EXISTS v_current_permits AS
SELECT
    p.permit_id,
    p.company_id,
    c.official_name,
    p.permit_number,
    p.permit_authority,
    p.permit_category,
    p.permit_year,
    p.issue_date,
    p.expiry_date,
    p.source,
    GROUP_CONCAT(pt.trade_name, ',') AS trade_names
FROM permits p
JOIN companies c ON c.company_id = p.company_id
LEFT JOIN permit_trades pt ON pt.permit_id = p.permit_id
WHERE p.current_flag = 1
GROUP BY p.permit_id
;

-- v_expiry_alerts: 90/60/30日以内に期限切れる許可証
CREATE VIEW IF NOT EXISTS v_expiry_alerts AS
SELECT
    p.permit_id,
    p.company_id,
    c.official_name,
    p.permit_number,
    p.expiry_date,
    CAST(julianday(p.expiry_date) - julianday('now','localtime') AS INTEGER) AS days_remaining,
    CASE
        WHEN julianday(p.expiry_date) - julianday('now','localtime') <= 0  THEN 'EXPIRED'
        WHEN julianday(p.expiry_date) - julianday('now','localtime') <= 30 THEN '30_DAYS'
        WHEN julianday(p.expiry_date) - julianday('now','localtime') <= 60 THEN '60_DAYS'
        WHEN julianday(p.expiry_date) - julianday('now','localtime') <= 90 THEN '90_DAYS'
    END AS alert_level,
    GROUP_CONCAT(pt.trade_name, ',') AS trade_names
FROM permits p
JOIN companies c ON c.company_id = p.company_id
LEFT JOIN permit_trades pt ON pt.permit_id = p.permit_id
WHERE p.current_flag = 1
  AND p.expiry_date IS NOT NULL
  AND p.expiry_date != 'UNCERTAIN'
  AND julianday(p.expiry_date) - julianday('now','localtime') <= 90
GROUP BY p.permit_id
ORDER BY days_remaining ASC
;
"""


def init_db(db_path: Optional[Path] = None) -> None:
    """DB初期化: テーブル + ビュー作成"""
    p = db_path or DB_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = get_connection(p)
    conn.executescript(_DDL)
    conn.executescript(_VIEWS)
    conn.close()
    print(f"[db] 初期化完了: {p}")


if __name__ == "__main__":
    init_db()
