-- Migration 008: classification_candidates / classification_runs (shadow mode)
-- フィンガープリント分類器の予測結果を pages を触らずに保存する。
-- promote 時のみ pages.doc_type_name を UPDATE する（手動ラベル除外）。

CREATE TABLE IF NOT EXISTS classification_runs (
  run_id TEXT PRIMARY KEY,
  classifier_version TEXT NOT NULL,
  fingerprint_version TEXT NOT NULL,
  mode TEXT NOT NULL DEFAULT 'shadow',         -- shadow / promoted
  pages_processed INTEGER,
  pages_high_conf INTEGER,
  pages_changed INTEGER,
  gold_precision REAL,
  gold_recall REAL,
  notes TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
);

CREATE TABLE IF NOT EXISTS classification_candidates (
  candidate_id INTEGER PRIMARY KEY AUTOINCREMENT,
  page_id INTEGER NOT NULL,
  run_id TEXT NOT NULL,
  classifier_version TEXT NOT NULL,
  predicted_label TEXT NOT NULL,
  confidence REAL NOT NULL,
  second_label TEXT,
  second_confidence REAL,
  evidence_json TEXT,
  previous_effective_label TEXT,
  would_change INTEGER NOT NULL DEFAULT 0,     -- 0=同じ / 1=変更
  manual_locked INTEGER NOT NULL DEFAULT 0,    -- 1=手動ラベルで保護されている
  created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
  FOREIGN KEY (page_id) REFERENCES pages(page_id),
  FOREIGN KEY (run_id) REFERENCES classification_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_cand_page ON classification_candidates(page_id);
CREATE INDEX IF NOT EXISTS idx_cand_run ON classification_candidates(run_id);
CREATE INDEX IF NOT EXISTS idx_cand_predicted ON classification_candidates(predicted_label);
CREATE INDEX IF NOT EXISTS idx_cand_changed ON classification_candidates(would_change);
