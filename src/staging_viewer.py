"""
staging_viewer.py — ページレベルドキュメント種類タグ付け + OCR編集ビューワー

3ペインレイアウト:
  左: ファイルリスト（展開可能なページサムネイル付き）
  中央: PDFページ画像ビューワー（ズーム/回転 + OCRオーバーレイ）
  右: ページ情報・ドキュメント種類選択・OCRフィールド編集・承認ボタン

Usage:
    python src/staging_viewer.py
    python src/staging_viewer.py --csv output/staging_permits_XXXXXXXX.csv
    python src/staging_viewer.py --port 8080
"""
from __future__ import annotations

import argparse
import base64
import csv
import io
import json
import sys
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlparse, unquote

try:
    import fitz  # PyMuPDF
    FITZ_AVAILABLE = True
except ImportError:
    FITZ_AVAILABLE = False
    print("[WARNING] PyMuPDF (fitz) not installed. PDF rendering unavailable.", file=sys.stderr)
    print("          Install with: pip install pymupdf", file=sys.stderr)

sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).parent.parent
INBOX_DIR = PROJECT_ROOT / "data" / "inbox"
SUCCESS_DIR = PROJECT_ROOT / "data" / "processed" / "success"
SKIP_DIR = PROJECT_ROOT / "data" / "processed" / "skip"
STAGING_DIR = PROJECT_ROOT / "output"

PAGE_TAGS_CSV = STAGING_DIR / "page_tags.csv"
OCR_OVERRIDES_CSV = STAGING_DIR / "permit_ocr_overrides.csv"

BADGE_COLORS = {
    "取引申請書": "#2196F3",
    "建設業許可証": "#f44336",
    "決算書": "#4CAF50",
    "会社案内": "#9C27B0",
    "工事経歴書": "#FF9800",
    "取引先一覧表": "#795548",
    "労働安全衛生誓約書": "#009688",
    "資格略字一覧": "#607D8B",
    "労働者名簿": "#E91E63",
    "その他/不明": "#9E9E9E",
}

DOC_TYPES = list(BADGE_COLORS.keys())

REASON_TO_DOCTYPE = {
    "新規継続取引申請書": "取引申請書",
    "取引申請": "取引申請書",
    "建設業許可": "建設業許可証",
    "決算書": "決算書",
    "会社案内": "会社案内",
    "工事経歴書": "工事経歴書",
    "取引先一覧": "取引先一覧表",
    "労働安全": "労働安全衛生誓約書",
    "資格略字": "資格略字一覧",
    "労働者名簿": "労働者名簿",
}

OCR_FIELDS = [
    ("company_name_raw", "会社名", "#60a5fa"),
    ("permit_authority_name", "行政庁", "#94a3b8"),
    ("permit_category", "般/特", "#94a3b8"),
    ("permit_number_full", "許可番号", "#f87171"),
    ("permit_year", "許可年", "#94a3b8"),
    ("issue_date", "許可日", "#94a3b8"),
    ("expiry_date", "有効期限", "#fb923c"),
    ("trade_categories", "業種", "#4ade80"),
]


# ─── Data helpers ─────────────────────────────────────────────────────────────

def find_latest_staging_csv() -> Path:
    csvs = sorted(STAGING_DIR.glob("staging_permits_*.csv"), reverse=True)
    if not csvs:
        raise FileNotFoundError("No staging CSV found in output/")
    return csvs[0]


def infer_doc_type(row: dict) -> str:
    """Infer document type from error_reason or parse_status."""
    if row.get("parse_status") == "OK":
        return "建設業許可証"
    reason = row.get("error_reason", "")
    for keyword, doc_type in REASON_TO_DOCTYPE.items():
        if keyword in reason:
            return doc_type
    return "その他/不明"


def load_staging_csv(csv_path: Path) -> list[dict]:
    rows = []
    with open(csv_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["_approved"] = row.get("_approved", "")
            rows.append(row)
    return rows


def load_page_tags() -> dict[tuple[str, int], str]:
    """Load page_tags.csv → {(source_file, page_no): doc_type}"""
    tags: dict[tuple[str, int], str] = {}
    if not PAGE_TAGS_CSV.exists():
        return tags
    with open(PAGE_TAGS_CSV, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                key = (row["file_id"], int(row["page_no"]))
                tags[key] = row["doc_type"]
            except (KeyError, ValueError):
                pass
    return tags


def save_page_tag(source_file: str, page_no: int, doc_type: str) -> None:
    """Upsert a row in page_tags.csv."""
    existing: list[dict] = []
    if PAGE_TAGS_CSV.exists():
        with open(PAGE_TAGS_CSV, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            existing = list(reader)

    updated = False
    for row in existing:
        if row.get("file_id") == source_file and row.get("page_no") == str(page_no):
            row["doc_type"] = doc_type
            row["updated_at"] = datetime.now().isoformat()
            updated = True
            break

    if not updated:
        existing.append({
            "file_id": source_file,
            "page_no": str(page_no),
            "doc_type": doc_type,
            "updated_at": datetime.now().isoformat(),
        })

    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    with open(PAGE_TAGS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["file_id", "page_no", "doc_type", "updated_at"])
        writer.writeheader()
        writer.writerows(existing)

    # Update in-memory cache
    AppState.page_tags[(source_file, page_no)] = doc_type


def load_ocr_overrides() -> dict[tuple[str, int], dict[str, str]]:
    """Load permit_ocr_overrides.csv → {(source_file, page_no): {field: value}}"""
    overrides: dict[tuple[str, int], dict[str, str]] = {}
    if not OCR_OVERRIDES_CSV.exists():
        return overrides
    with open(OCR_OVERRIDES_CSV, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                key = (row["file_id"], int(row["page_no"]))
                if key not in overrides:
                    overrides[key] = {}
                overrides[key][row["field_name"]] = row["value"]
            except (KeyError, ValueError):
                pass
    return overrides


def save_ocr_overrides(source_file: str, page_no: int, fields: dict[str, str]) -> None:
    """Upsert OCR field overrides for (source_file, page_no)."""
    existing: list[dict] = []
    if OCR_OVERRIDES_CSV.exists():
        with open(OCR_OVERRIDES_CSV, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            existing = list(reader)

    now = datetime.now().isoformat()
    for field_name, value in fields.items():
        updated = False
        for row in existing:
            if (row.get("file_id") == source_file
                    and row.get("page_no") == str(page_no)
                    and row.get("field_name") == field_name):
                row["value"] = value
                row["updated_at"] = now
                updated = True
                break
        if not updated:
            existing.append({
                "file_id": source_file,
                "page_no": str(page_no),
                "field_name": field_name,
                "value": value,
                "updated_at": now,
            })

    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    with open(OCR_OVERRIDES_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["file_id", "page_no", "field_name", "value", "updated_at"])
        writer.writeheader()
        writer.writerows(existing)

    # Update in-memory cache
    key = (source_file, page_no)
    if key not in AppState.ocr_overrides:
        AppState.ocr_overrides[key] = {}
    AppState.ocr_overrides[key].update(fields)


def find_pdf_path(source_file: str) -> Path | None:
    candidates = [
        SUCCESS_DIR / source_file,
        SKIP_DIR / source_file,
        INBOX_DIR / source_file,
    ]
    for p in candidates:
        if p.exists():
            return p
    for directory in [SUCCESS_DIR, SKIP_DIR, INBOX_DIR]:
        if directory.exists():
            for p in directory.iterdir():
                if p.name == source_file:
                    return p
    return None


def get_page_count(pdf_path: Path) -> int:
    if not FITZ_AVAILABLE:
        return 1
    try:
        doc = fitz.open(str(pdf_path))
        count = doc.page_count
        doc.close()
        return count
    except Exception:
        return 1


def render_pdf_page(pdf_path: Path, page_no: int, dpi: int = 150) -> bytes | None:
    if not FITZ_AVAILABLE:
        return None
    try:
        doc = fitz.open(str(pdf_path))
        page_idx = max(0, min(page_no - 1, doc.page_count - 1))
        page = doc[page_idx]
        zoom = dpi / 72.0
        mat = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat)
        img_bytes = pix.tobytes("jpeg")
        doc.close()
        return img_bytes
    except Exception as e:
        print(f"[ERROR] render_pdf_page: {e}", file=sys.stderr)
        return None


def render_page_thumbnail(pdf_path: Path, page_no: int = 1, size: int = 60) -> bytes | None:
    """Render a specific page as thumbnail bytes."""
    if not FITZ_AVAILABLE:
        return None
    try:
        doc = fitz.open(str(pdf_path))
        page_idx = max(0, min(page_no - 1, doc.page_count - 1))
        page = doc[page_idx]
        zoom = size / max(page.rect.width, page.rect.height)
        mat = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat)
        img_bytes = pix.tobytes("jpeg")
        doc.close()
        return img_bytes
    except Exception:
        return None


# ─── State (shared across requests) ──────────────────────────────────────────

class AppState:
    records: list[dict] = []
    csv_path: Path = Path()
    approved_path: Path = Path()

    # Cache: source_file -> {"page_count": N, "path": str, "exists": bool}
    file_cache: dict[str, dict] = {}

    # Page thumbnail cache: (source_file, page_no) -> JPEG bytes
    thumb_cache: dict[tuple[str, int], bytes] = {}

    # Page tags: (source_file, page_no) -> doc_type
    page_tags: dict[tuple[str, int], str] = {}

    # OCR overrides: (source_file, page_no) -> {field_name: value}
    ocr_overrides: dict[tuple[str, int], dict[str, str]] = {}

    @classmethod
    def get_file_info(cls, source_file: str) -> dict:
        if source_file not in cls.file_cache:
            path = find_pdf_path(source_file)
            if path and path.exists():
                page_count = get_page_count(path)
                cls.file_cache[source_file] = {
                    "path": str(path),
                    "page_count": page_count,
                    "exists": True,
                }
            else:
                cls.file_cache[source_file] = {
                    "path": "",
                    "page_count": 0,
                    "exists": False,
                }
        return cls.file_cache[source_file]

    @classmethod
    def get_thumb(cls, source_file: str, page_no: int) -> bytes | None:
        key = (source_file, page_no)
        if key not in cls.thumb_cache:
            info = cls.get_file_info(source_file)
            if info["exists"]:
                thumb = render_page_thumbnail(Path(info["path"]), page_no, size=60)
                if thumb:
                    cls.thumb_cache[key] = thumb
                    return thumb
            return None
        return cls.thumb_cache[key]

    @classmethod
    def get_page_tag(cls, source_file: str, page_no: int) -> str:
        """Get doc type for a specific page, with fallback to file-level infer."""
        tag = cls.page_tags.get((source_file, page_no))
        if tag:
            return tag
        # Fallback: infer from staging CSV
        for rec in cls.records:
            if rec.get("source_file") == source_file:
                return infer_doc_type(rec)
        return "その他/不明"

    @classmethod
    def get_ocr_data(cls, source_file: str, page_no: int) -> dict[str, str]:
        """Get OCR data for a page: staging CSV + overrides applied."""
        base: dict[str, str] = {}
        for rec in cls.records:
            if rec.get("source_file") == source_file:
                for fkey, _, _ in OCR_FIELDS:
                    base[fkey] = rec.get(fkey, "")
                break
        overrides = cls.ocr_overrides.get((source_file, page_no), {})
        base.update(overrides)
        return base

    @classmethod
    def save_approved(cls):
        approved = [r for r in cls.records if r.get("_approved") == "true"]
        if not approved:
            return
        path = cls.approved_path
        fieldnames = list(approved[0].keys())
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(approved)


# ─── HTML Page ────────────────────────────────────────────────────────────────

HTML_PAGE = r"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>許可証ドキュメントビューワー</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: "Segoe UI", "Meiryo", sans-serif; background: #f5f7fa; overflow: hidden; }

/* ── Header ── */
.header {
  background: #1b3d6f;
  color: #fff;
  padding: 10px 16px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  height: 48px;
  flex-shrink: 0;
}
.header h1 { font-size: 15px; font-weight: 600; letter-spacing: 0.3px; }
.header-right { display: flex; align-items: center; gap: 12px; font-size: 12px; color: #93c5fd; }
.hbadge { padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 600; }
.hbadge-ok { background: #10b981; color: #fff; }
.hbadge-skip { background: #6b7280; color: #fff; }
.hbadge-approved { background: #f59e0b; color: #fff; }

/* ── Layout ── */
.layout { display: flex; height: calc(100vh - 48px); }

/* ── Left Pane ── */
.left-pane {
  width: 220px; min-width: 220px;
  background: #fff;
  border-right: 1px solid #e2e8f0;
  display: flex; flex-direction: column; overflow: hidden;
}
.left-pane-header {
  padding: 8px 12px;
  font-size: 10px; font-weight: 700; color: #64748b;
  text-transform: uppercase; letter-spacing: 0.6px;
  border-bottom: 1px solid #f1f5f9; background: #f8fafc;
  flex-shrink: 0;
}
.file-list { overflow-y: auto; flex: 1; }

/* ── File header row ── */
.file-header {
  display: flex; align-items: center; gap: 6px;
  padding: 7px 10px 5px;
  border-bottom: 1px solid #f1f5f9;
  cursor: pointer;
  transition: background 0.1s;
  user-select: none;
}
.file-header:hover { background: #f8fafc; }
.file-header.active-file { background: #eff6ff; border-left: 3px solid #1b3d6f; padding-left: 7px; }
.file-expand-arrow {
  font-size: 10px; color: #94a3b8; transition: transform 0.15s;
  flex-shrink: 0; width: 12px; text-align: center;
}
.file-expand-arrow.open { transform: rotate(90deg); }
.file-icon { font-size: 14px; flex-shrink: 0; }
.file-meta { flex: 1; min-width: 0; }
.file-name {
  font-size: 11px; color: #374151; font-weight: 500;
  overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
  margin-bottom: 2px;
}
.file-sub {
  font-size: 10px; color: #9ca3af;
  display: flex; gap: 4px; align-items: center; flex-wrap: wrap;
}
.ocr-badge {
  font-size: 9px; padding: 1px 4px;
  border-radius: 6px; color: #fff; font-weight: 700;
}

/* ── Page thumbnail rows (nested under file) ── */
.page-thumb-list { background: #f8fafc; border-bottom: 1px solid #f1f5f9; }
.page-thumb-row {
  display: flex; align-items: center; gap: 6px;
  padding: 4px 8px 4px 22px;
  cursor: pointer;
  transition: background 0.1s;
  border-bottom: 1px solid #f1f5f9;
}
.page-thumb-row:hover { background: #f0f4ff; }
.page-thumb-row.active-page { background: #dbeafe; }
.page-thumb-img {
  width: 36px; height: 46px;
  border: 1px solid #e2e8f0; border-radius: 2px;
  object-fit: cover; flex-shrink: 0;
  background: #e5e7eb;
}
.page-thumb-info { flex: 1; min-width: 0; }
.page-thumb-num { font-size: 10px; color: #374151; font-weight: 600; }
.page-doc-badge {
  display: inline-block;
  font-size: 8px; padding: 1px 4px;
  border-radius: 6px; color: #fff; font-weight: 700;
  max-width: 100px; overflow: hidden; text-overflow: ellipsis;
  white-space: nowrap; vertical-align: middle;
}

/* ── Center Pane ── */
.center-pane {
  flex: 1; min-width: 0;
  background: #e2e8f0;
  display: flex; flex-direction: column; overflow: hidden;
}
.toolbar {
  background: #1e293b;
  padding: 6px 12px;
  display: flex; align-items: center; gap: 6px;
  flex-shrink: 0;
}
.tb-btn {
  background: #334155; color: #e2e8f0;
  border: none; border-radius: 4px;
  padding: 4px 9px; font-size: 13px;
  cursor: pointer; transition: background 0.1s;
  font-family: inherit;
}
.tb-btn:hover:not(:disabled) { background: #475569; }
.tb-btn:disabled { opacity: 0.35; cursor: default; }
.tb-btn.active { background: #2563eb; color: #fff; }
.tb-label {
  color: #e2e8f0; font-size: 12px; min-width: 44px;
  text-align: center;
}
.tb-sep { width: 1px; height: 20px; background: #475569; margin: 0 2px; }
.page-nav { display: flex; align-items: center; gap: 4px; color: #e2e8f0; font-size: 12px; }
#page-label { min-width: 50px; text-align: center; color: #f1f5f9; font-size: 12px; }

.image-scroll {
  flex: 1; overflow: auto;
  display: flex; justify-content: center; align-items: flex-start;
  padding: 16px;
  position: relative;
}
.image-scroll-inner {
  display: flex; justify-content: center;
}
#pdf-image {
  max-width: 100%;
  box-shadow: 0 4px 16px rgba(0,0,0,0.25);
  border-radius: 3px;
  transition: opacity 0.15s;
  transform-origin: top center;
  display: block;
}
.no-image-msg {
  display: flex; flex-direction: column;
  align-items: center; justify-content: center;
  color: #64748b; gap: 8px;
  font-size: 14px; height: 200px; width: 100%;
}
.no-image-icon { font-size: 36px; }

/* ── OCR Overlay ── */
.ocr-overlay {
  display: none;
  position: absolute;
  bottom: 16px; left: 16px;
  background: rgba(15,23,42,0.88);
  border-radius: 6px;
  padding: 10px 14px;
  min-width: 260px;
  max-width: 380px;
  backdrop-filter: blur(4px);
  z-index: 10;
}
.ocr-overlay.visible { display: block; }
.ocr-overlay-title {
  font-size: 10px; font-weight: 700;
  color: #94a3b8; text-transform: uppercase;
  letter-spacing: 0.5px; margin-bottom: 8px;
}
.ocr-field { display: flex; align-items: baseline; gap: 8px; margin-bottom: 5px; }
.ocr-field-label { font-size: 10px; color: #94a3b8; width: 56px; flex-shrink: 0; }
.ocr-field-value {
  font-size: 12px; font-weight: 600; color: #f1f5f9;
}
.ocr-field-value.empty { color: #475569; font-style: italic; font-weight: 400; }
.ocr-field-value.blue { color: #60a5fa; }
.ocr-field-value.red { color: #f87171; }
.ocr-field-value.orange { color: #fb923c; }
.ocr-field-value.green { color: #4ade80; }

/* ── Right Pane ── */
.right-pane {
  width: 360px; min-width: 300px;
  background: #fff;
  border-left: 1px solid #e2e8f0;
  display: flex; flex-direction: column; overflow: hidden;
}
.right-header {
  padding: 12px 14px 10px;
  border-bottom: 1px solid #f1f5f9;
  background: #f8fafc;
  flex-shrink: 0;
}
.right-title {
  font-size: 13px; font-weight: 700; color: #1e293b;
  overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
.right-body { flex: 1; overflow-y: auto; padding: 12px 14px; }

.section-title {
  font-size: 10px; font-weight: 700; color: #64748b;
  text-transform: uppercase; letter-spacing: 0.5px;
  margin: 14px 0 8px; padding-top: 12px;
  border-top: 1px solid #f1f5f9;
}
.section-title:first-child { margin-top: 0; padding-top: 0; border-top: none; }

.page-info-grid {
  display: grid; grid-template-columns: 70px 1fr;
  gap: 4px 8px; font-size: 12px;
}
.page-info-key { color: #6b7280; }
.page-info-val { color: #1e293b; font-weight: 500; word-break: break-all; }

.doc-type-select {
  width: 100%;
  padding: 7px 10px;
  border: 1.5px solid #e2e8f0; border-radius: 6px;
  font-size: 13px; font-family: inherit;
  background: #fff; color: #1e293b;
  cursor: pointer;
  appearance: none;
  background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='8' viewBox='0 0 12 8'%3E%3Cpath fill='%236b7280' d='M1 1l5 5 5-5'/%3E%3C/svg%3E");
  background-repeat: no-repeat;
  background-position: right 10px center;
  padding-right: 28px;
}
.doc-type-select:focus { outline: none; border-color: #1b3d6f; }

.non-permit-msg {
  margin-top: 12px;
  padding: 10px 12px;
  background: #f8fafc; border-radius: 6px;
  font-size: 12px; color: #64748b;
  border: 1px solid #e2e8f0;
}

.ocr-edit-field { margin-bottom: 8px; }
.ocr-edit-label {
  font-size: 11px; color: #6b7280; margin-bottom: 3px;
  display: flex; align-items: center; gap: 5px;
}
.color-dot { width: 7px; height: 7px; border-radius: 50%; flex-shrink: 0; }
.ocr-edit-input {
  width: 100%;
  padding: 5px 8px;
  border: 1.5px solid #e2e8f0; border-radius: 5px;
  font-size: 12px; font-family: inherit;
  color: #1e293b; background: #fff;
  transition: border-color 0.1s;
}
.ocr-edit-input:focus { outline: none; border-color: #2563eb; }

.action-buttons {
  padding: 12px 14px;
  border-top: 1px solid #f1f5f9;
  display: flex; flex-direction: column; gap: 6px;
  flex-shrink: 0;
  background: #fff;
}
.btn-approve {
  padding: 9px 14px; border-radius: 6px;
  border: none; cursor: pointer;
  font-size: 13px; font-weight: 600;
  font-family: inherit;
  background: #059669; color: #fff;
  transition: background 0.15s;
}
.btn-approve:hover { background: #047857; }
.btn-approve.approved-state { background: #f59e0b; }
.btn-approve.approved-state:hover { background: #d97706; }
.btn-skip {
  padding: 7px 14px; border-radius: 6px;
  border: 1.5px solid #e2e8f0; cursor: pointer;
  font-size: 12px; font-family: inherit;
  background: #fff; color: #6b7280;
  transition: background 0.1s;
}
.btn-skip:hover { background: #f8fafc; }
.btn-ocr-run {
  padding: 7px 14px; border-radius: 6px;
  border: 1.5px solid #e2e8f0; cursor: pointer;
  font-size: 12px; font-family: inherit;
  background: #fff; color: #1b3d6f;
  transition: background 0.1s;
}
.btn-ocr-run:hover { background: #eff6ff; }

.status-pill {
  display: inline-block; padding: 2px 8px;
  border-radius: 10px; font-size: 11px; font-weight: 600;
}
.status-pill.ok { background: #d1fae5; color: #065f46; }
.status-pill.skip { background: #f3f4f6; color: #374151; }
.status-pill.approved { background: #fef3c7; color: #92400e; }
</style>
</head>
<body>
<div class="header">
  <h1>📋 許可証ドキュメントビューワー</h1>
  <div class="header-right">
    <span id="total-count" style="color:#e2e8f0">—</span> 件
    <span id="ok-count" class="hbadge hbadge-ok">—</span>
    <span id="skip-count" class="hbadge hbadge-skip">—</span>
    <span id="approved-count" class="hbadge hbadge-approved">—</span>
  </div>
</div>

<div class="layout">
  <!-- Left Pane -->
  <div class="left-pane">
    <div class="left-pane-header">ファイル一覧</div>
    <div class="file-list" id="file-list">
      <div style="padding:12px;color:#9ca3af;font-size:12px;">読み込み中...</div>
    </div>
  </div>

  <!-- Center Pane -->
  <div class="center-pane">
    <div class="toolbar">
      <button class="tb-btn" onclick="zoomOut()" title="縮小">−</button>
      <span class="tb-label" id="zoom-label">100%</span>
      <button class="tb-btn" onclick="zoomIn()" title="拡大">＋</button>
      <button class="tb-btn" onclick="zoomFit()" title="フィット" style="font-size:11px">⤡</button>
      <div class="tb-sep"></div>
      <button class="tb-btn" onclick="rotateLeft()" title="左回転">↺</button>
      <button class="tb-btn" onclick="rotateRight()" title="右回転">↻</button>
      <div class="tb-sep"></div>
      <div class="page-nav">
        <button class="tb-btn" id="prev-page-btn" onclick="prevPage()" disabled>←</button>
        <span id="page-label">— / —</span>
        <button class="tb-btn" id="next-page-btn" onclick="nextPage()" disabled>→</button>
      </div>
      <div class="tb-sep"></div>
      <button class="tb-btn" id="overlay-btn" onclick="toggleOverlay()" title="OCR表示">● OCR表示</button>
    </div>

    <div class="image-scroll" id="image-scroll">
      <div class="image-scroll-inner">
        <img id="pdf-image" src="" alt="" onload="onImageLoad()" onerror="onImageError()" style="display:none">
        <div class="no-image-msg" id="no-image-msg">
          <span class="no-image-icon">📄</span>
          <span>ファイルを選択してください</span>
        </div>
      </div>
      <div class="ocr-overlay" id="ocr-overlay">
        <div class="ocr-overlay-title">OCR抽出データ</div>
        <div id="ocr-overlay-grid"></div>
      </div>
    </div>
  </div>

  <!-- Right Pane -->
  <div class="right-pane">
    <div class="right-header">
      <div class="right-title" id="right-header">ページを選択してください</div>
    </div>
    <div class="right-body" id="right-body">
      <div style="color:#9ca3af;font-size:12px;padding:8px 0;">ファイルリストからページを選択してください。</div>
    </div>
    <div class="action-buttons" id="action-buttons" style="display:none">
      <button class="btn-approve" id="btn-approve" onclick="approveRecord()">✓ 確認OK・登録</button>
      <div style="display:flex;gap:6px">
        <button class="btn-skip" onclick="skipRecord()" style="flex:1">スキップ →</button>
        <button class="btn-ocr-run" onclick="runOcr()" style="flex:1">OCR実行</button>
      </div>
    </div>
  </div>
</div>

<script>
var state = {
  files: [],
  selectedFile: null,
  selectedPage: 1,
  totalPages: 1,
  zoom: 1.0,
  rotation: 0,
  overlayVisible: false,
  expandedFiles: {},   // source_file -> bool
  pageTagsCache: {},   // "file|page" -> doc_type
};

var BADGE_COLORS = {
  '取引申請書': '#2196F3',
  '建設業許可証': '#f44336',
  '決算書': '#4CAF50',
  '会社案内': '#9C27B0',
  '工事経歴書': '#FF9800',
  '取引先一覧表': '#795548',
  '労働安全衛生誓約書': '#009688',
  '資格略字一覧': '#607D8B',
  '労働者名簿': '#E91E63',
  'その他/不明': '#9E9E9E',
};
var BADGE_SHORT = {
  '取引申請書': '取引',
  '建設業許可証': '許可',
  '決算書': '決算',
  '会社案内': '案内',
  '工事経歴書': '工歴',
  '取引先一覧表': '一覧',
  '労働安全衛生誓約書': '安全',
  '資格略字一覧': '資格',
  '労働者名簿': '名簿',
  'その他/不明': '他',
};
var DOC_TYPES = ["取引申請書","建設業許可証","決算書","会社案内","工事経歴書","取引先一覧表","労働安全衛生誓約書","資格略字一覧","労働者名簿","その他/不明"];

// ── Boot ───────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', function() {
  loadFiles();
  document.addEventListener('keydown', function(e) {
    if (e.target && (e.target.tagName === 'INPUT' || e.target.tagName === 'SELECT' || e.target.tagName === 'TEXTAREA')) return;
    if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') { prevPage(); e.preventDefault(); }
    if (e.key === 'ArrowRight' || e.key === 'ArrowDown') { nextPage(); e.preventDefault(); }
  });
});

function loadFiles() {
  fetch('/api/files')
    .then(function(r) { return r.json(); })
    .then(function(data) {
      state.files = data.files;
      // Build page tags cache from returned data
      data.files.forEach(function(f) {
        if (f.page_tags) {
          Object.keys(f.page_tags).forEach(function(pn) {
            state.pageTagsCache[f.source_file + '|' + pn] = f.page_tags[pn];
          });
        }
      });
      renderFileList();
      updateHeaderCounts(data.stats);
      if (state.files.length > 0) {
        var first = state.files[0];
        state.expandedFiles[first.source_file] = true;
        selectPage(first.source_file, 1);
      }
    })
    .catch(function() {
      document.getElementById('file-list').innerHTML =
        '<div style="padding:12px;color:#ef4444;font-size:12px;">ファイル一覧の取得に失敗しました</div>';
    });
}

function updateHeaderCounts(stats) {
  if (!stats) return;
  document.getElementById('total-count').textContent = stats.total || 0;
  document.getElementById('ok-count').textContent = 'OK: ' + (stats.ok || 0);
  document.getElementById('skip-count').textContent = 'SKIP: ' + (stats.skip || 0);
  document.getElementById('approved-count').textContent = '承認済: ' + (stats.approved || 0);
}

// ── File List Rendering ────────────────────────────────────────────────────
function renderFileList() {
  var html = '';
  state.files.forEach(function(f) {
    var isActiveFile = f.source_file === state.selectedFile;
    var isExpanded = !!state.expandedFiles[f.source_file];
    var pageCount = f.page_count || 0;
    var hasOcr = f.parse_status === 'OK';
    var arrowClass = isExpanded ? 'file-expand-arrow open' : 'file-expand-arrow';

    var ocrBadge = hasOcr
      ? '<span class="ocr-badge" style="background:#10b981">OCR✓</span>'
      : '';
    var fileClass = 'file-header' + (isActiveFile ? ' active-file' : '');

    html += '<div class="' + fileClass + '" onclick="toggleExpand(\'' + escJs(f.source_file) + '\')">';
    html += '<span class="' + arrowClass + '" id="arrow_' + escId(f.source_file) + '">▶</span>';
    html += '<span class="file-icon">📄</span>';
    html += '<div class="file-meta">';
    html += '<div class="file-name" title="' + escHtml(f.source_file) + '">' + escHtml(getShortName(f.source_file)) + '</div>';
    html += '<div class="file-sub">';
    html += '<span>' + pageCount + 'p</span>';
    html += ocrBadge;
    html += '</div>';
    html += '</div></div>';

    if (isExpanded) {
      html += '<div class="page-thumb-list" id="thumbs_' + escId(f.source_file) + '">';
      for (var p = 1; p <= pageCount; p++) {
        var isActivePage = (f.source_file === state.selectedFile && p === state.selectedPage);
        var docType = getPageDocType(f.source_file, p, f);
        var badgeColor = BADGE_COLORS[docType] || '#9E9E9E';
        var badgeShort = BADGE_SHORT[docType] || '他';
        var rowClass = 'page-thumb-row' + (isActivePage ? ' active-page' : '');
        var thumbSrc = '/api/thumb/' + encodeURIComponent(f.source_file) + '/' + p;

        html += '<div class="' + rowClass + '" id="prow_' + escId(f.source_file) + '_' + p + '"';
        html += ' onclick="selectPage(\'' + escJs(f.source_file) + '\',' + p + ')">';
        html += '<img class="page-thumb-img" src="' + thumbSrc + '" alt="P' + p + '" loading="lazy">';
        html += '<div class="page-thumb-info">';
        html += '<div class="page-thumb-num">P' + p + '</div>';
        html += '<span class="page-doc-badge" style="background:' + badgeColor + '">' + escHtml(badgeShort) + '</span>';
        html += '</div></div>';
      }
      html += '</div>';
    }
  });

  document.getElementById('file-list').innerHTML = html ||
    '<div style="padding:12px;color:#9ca3af;font-size:12px;">ファイルがありません</div>';
}

function getPageDocType(sourceFile, pageNo, fileObj) {
  var cacheKey = sourceFile + '|' + pageNo;
  if (state.pageTagsCache[cacheKey]) return state.pageTagsCache[cacheKey];
  if (fileObj && fileObj.page_tags && fileObj.page_tags[pageNo]) {
    return fileObj.page_tags[pageNo];
  }
  // Fall back to file-level inferred type
  if (fileObj) {
    return fileObj.doc_type || 'その他/不明';
  }
  return 'その他/不明';
}

function toggleExpand(sourceFile) {
  state.expandedFiles[sourceFile] = !state.expandedFiles[sourceFile];
  renderFileList();
  if (state.expandedFiles[sourceFile]) {
    selectPage(sourceFile, 1);
  }
}

function getFile(sourceFile) {
  for (var i = 0; i < state.files.length; i++) {
    if (state.files[i].source_file === sourceFile) return state.files[i];
  }
  return null;
}

function getShortName(filename) {
  return filename.replace(/^\d{8}_\d{6}_/, '').replace(/\.pdf$/i, '');
}

// ── Page Selection ─────────────────────────────────────────────────────────
function selectPage(sourceFile, pageNo) {
  state.selectedFile = sourceFile;
  state.selectedPage = pageNo;
  state.zoom = 1.0;
  state.rotation = 0;

  var file = getFile(sourceFile);
  if (!file) return;
  state.totalPages = file.page_count || 1;

  renderFileList();
  loadPageImage();
  loadRightPanel(sourceFile, pageNo, file);
  scrollThumbIntoView(sourceFile, pageNo);
}

function scrollThumbIntoView(sourceFile, pageNo) {
  var id = 'prow_' + escId(sourceFile) + '_' + pageNo;
  var el = document.getElementById(id);
  if (el) el.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

// ── Right Panel ────────────────────────────────────────────────────────────
function loadRightPanel(sourceFile, pageNo, fileObj) {
  var docType = getPageDocType(sourceFile, pageNo, fileObj);
  var isPermit = (docType === '建設業許可証');

  document.getElementById('right-header').textContent =
    getShortName(sourceFile) + ' — P' + pageNo;

  var rec = (fileObj.records && fileObj.records[0]) ? fileObj.records[0] : {};
  var isApproved = rec._approved === 'true';
  var isOk = rec.parse_status === 'OK';

  var statusClass = isApproved ? 'approved' : (isOk ? 'ok' : 'skip');
  var statusText = isApproved ? '✓ 承認済' : (isOk ? '✓ OK' : 'SKIP');

  // Doc type dropdown
  var docTypeOpts = DOC_TYPES.map(function(t) {
    return '<option value="' + escHtml(t) + '"' + (t === docType ? ' selected' : '') + '>' + escHtml(t) + '</option>';
  }).join('');

  var html = '';

  // ── Page Info ──
  html += '<div class="section-title">ページ情報</div>';
  html += '<div class="page-info-grid">';
  html += '<span class="page-info-key">ファイル</span><span class="page-info-val" title="' + escHtml(sourceFile) + '">' + escHtml(getShortName(sourceFile)) + '</span>';
  html += '<span class="page-info-key">ページ</span><span class="page-info-val">' + pageNo + ' / ' + state.totalPages + '</span>';
  html += '<span class="page-info-key">ステータス</span><span class="page-info-val"><span class="status-pill ' + statusClass + '">' + statusText + '</span></span>';
  html += '</div>';

  // ── Doc Type ──
  html += '<div class="section-title">ドキュメント種類</div>';
  html += '<select class="doc-type-select" id="doc-type-select" onchange="updatePageDocType(this.value)">';
  html += docTypeOpts;
  html += '</select>';

  // ── OCR Fields or message ──
  if (isPermit) {
    html += '<div class="section-title">OCR抽出データ（編集可）</div>';
    // Fetch OCR data from server and populate
    html += '<div id="ocr-fields-container">';
    html += '<div style="color:#9ca3af;font-size:11px;padding:4px 0;">読み込み中...</div>';
    html += '</div>';
  } else {
    html += '<div class="non-permit-msg">このページは許可証ではありません。<br>ドキュメント種別のみ設定できます。</div>';
  }

  document.getElementById('right-body').innerHTML = html;

  // Show or hide action buttons
  document.getElementById('action-buttons').style.display = isPermit ? 'flex' : 'none';
  document.getElementById('action-buttons').style.flexDirection = 'column';

  var approveBtn = document.getElementById('btn-approve');
  if (approveBtn) {
    if (isApproved) {
      approveBtn.textContent = '✓ 承認済（再登録）';
      approveBtn.className = 'btn-approve approved-state';
    } else {
      approveBtn.textContent = '✓ 確認OK・登録';
      approveBtn.className = 'btn-approve';
    }
  }

  if (isPermit) {
    loadOcrFields(sourceFile, pageNo);
  }
}

function loadOcrFields(sourceFile, pageNo) {
  fetch('/api/ocr/' + encodeURIComponent(sourceFile))
    .then(function(r) { return r.json(); })
    .then(function(data) {
      renderOcrFields(data, sourceFile, pageNo);
      updateOcrOverlay(data);
    })
    .catch(function() {
      var el = document.getElementById('ocr-fields-container');
      if (el) el.innerHTML = '<div style="color:#ef4444;font-size:11px;">OCRデータの取得に失敗しました</div>';
    });
}

function renderOcrFields(ocrData, sourceFile, pageNo) {
  var container = document.getElementById('ocr-fields-container');
  if (!container) return;

  var fieldDefs = [
    { key: 'company_name_raw', label: '会社名', color: '#60a5fa' },
    { key: 'permit_authority_name', label: '行政庁', color: '#94a3b8' },
    { key: 'permit_category', label: '般/特', color: '#94a3b8' },
    { key: 'permit_number_full', label: '許可番号', color: '#f87171' },
    { key: 'permit_year', label: '許可年', color: '#94a3b8' },
    { key: 'issue_date', label: '許可日', color: '#94a3b8' },
    { key: 'expiry_date', label: '有効期限', color: '#fb923c' },
    { key: 'trade_categories', label: '業種', color: '#4ade80' },
  ];

  var html = '';
  fieldDefs.forEach(function(fd) {
    var val = ocrData[fd.key] || '';
    html += '<div class="ocr-edit-field">';
    html += '<div class="ocr-edit-label">';
    html += '<span class="color-dot" style="background:' + fd.color + '"></span>';
    html += escHtml(fd.label) + ':';
    html += '</div>';
    html += '<input type="text" class="ocr-edit-input" name="' + fd.key + '" id="field_' + fd.key + '" value="' + escHtml(val) + '">';
    html += '</div>';
  });

  container.innerHTML = html;
}

// ── Doc Type Update ────────────────────────────────────────────────────────
function updatePageDocType(docType) {
  if (!state.selectedFile) return;
  var pageNo = state.selectedPage;

  // Update cache
  var cacheKey = state.selectedFile + '|' + pageNo;
  state.pageTagsCache[cacheKey] = docType;

  // Update file object's page_tags
  var file = getFile(state.selectedFile);
  if (file) {
    if (!file.page_tags) file.page_tags = {};
    file.page_tags[pageNo] = docType;
  }

  // Save to server
  fetch('/api/tag', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ filename: state.selectedFile, page_no: pageNo, doc_type: docType })
  });

  // Re-render file list thumbnail badge and right panel
  renderFileList();
  var fileObj = getFile(state.selectedFile);
  if (fileObj) loadRightPanel(state.selectedFile, pageNo, fileObj);
}

// ── Image Viewer ───────────────────────────────────────────────────────────
function loadPageImage() {
  if (!state.selectedFile) return;
  var file = getFile(state.selectedFile);
  if (!file || !file.pdf_exists) {
    document.getElementById('pdf-image').style.display = 'none';
    var msg = document.getElementById('no-image-msg');
    msg.style.display = 'flex';
    msg.innerHTML = '<span class="no-image-icon">⚠️</span><span>PDFファイルが見つかりません</span>';
    updatePageNav();
    return;
  }

  document.getElementById('no-image-msg').style.display = 'none';
  var img = document.getElementById('pdf-image');
  img.style.display = 'block';
  img.style.opacity = '0.4';
  img.src = '/api/image/' + encodeURIComponent(state.selectedFile) + '/' + state.selectedPage + '?dpi=150';
  updatePageNav();
  updateZoomDisplay();
}

function onImageLoad() {
  var img = document.getElementById('pdf-image');
  img.style.opacity = '1';
  applyTransform();
}

function onImageError() {
  var img = document.getElementById('pdf-image');
  img.style.display = 'none';
  var msg = document.getElementById('no-image-msg');
  msg.style.display = 'flex';
  msg.innerHTML = '<span class="no-image-icon">⚠️</span><span>ページの読み込みに失敗しました</span>';
}

function applyTransform() {
  var img = document.getElementById('pdf-image');
  img.style.transform = 'rotate(' + state.rotation + 'deg)';
  img.style.width = Math.round(state.zoom * 100) + '%';
}

function updatePageNav() {
  document.getElementById('page-label').textContent = state.selectedPage + ' / ' + state.totalPages;
  document.getElementById('prev-page-btn').disabled = state.selectedPage <= 1;
  document.getElementById('next-page-btn').disabled = state.selectedPage >= state.totalPages;
}

function prevPage() {
  if (state.selectedPage > 1) {
    selectPage(state.selectedFile, state.selectedPage - 1);
  }
}

function nextPage() {
  if (state.selectedPage < state.totalPages) {
    selectPage(state.selectedFile, state.selectedPage + 1);
  }
}

function zoomIn() { state.zoom = Math.min(4.0, state.zoom + 0.25); applyTransform(); updateZoomDisplay(); }
function zoomOut() { state.zoom = Math.max(0.25, state.zoom - 0.25); applyTransform(); updateZoomDisplay(); }
function zoomFit() { state.zoom = 1.0; applyTransform(); updateZoomDisplay(); }
function updateZoomDisplay() { document.getElementById('zoom-label').textContent = Math.round(state.zoom * 100) + '%'; }
function rotateLeft() { state.rotation = (state.rotation - 90 + 360) % 360; applyTransform(); }
function rotateRight() { state.rotation = (state.rotation + 90) % 360; applyTransform(); }

// ── OCR Overlay ────────────────────────────────────────────────────────────
function updateOcrOverlay(rec) {
  var fields = [
    { label: '会社名', key: 'company_name_raw', cls: 'blue' },
    { label: '許可番号', key: 'permit_number_full', cls: 'red' },
    { label: '有効期限', key: 'expiry_date', cls: 'orange' },
    { label: '業種', key: 'trade_categories', cls: 'green' },
  ];
  var html = '';
  fields.forEach(function(f) {
    var val = rec[f.key] || '';
    html += '<div class="ocr-field">';
    html += '<span class="ocr-field-label">' + escHtml(f.label) + '</span>';
    html += '<span class="ocr-field-value ' + (val ? f.cls : 'empty') + '">' + (val ? escHtml(val) : '（未抽出）') + '</span>';
    html += '</div>';
  });
  document.getElementById('ocr-overlay-grid').innerHTML = html;
}

function toggleOverlay() {
  state.overlayVisible = !state.overlayVisible;
  var overlay = document.getElementById('ocr-overlay');
  var btn = document.getElementById('overlay-btn');
  overlay.classList.toggle('visible', state.overlayVisible);
  btn.classList.toggle('active', state.overlayVisible);
}

// ── Approve ────────────────────────────────────────────────────────────────
function approveRecord() {
  if (!state.selectedFile) return;
  var file = getFile(state.selectedFile);
  if (!file) return;

  var fields = {};
  ['company_name_raw','permit_authority_name','permit_category','permit_number_full',
   'permit_year','issue_date','expiry_date','trade_categories'].forEach(function(key) {
    var el = document.getElementById('field_' + key);
    if (el) fields[key] = el.value;
  });

  var docType = document.getElementById('doc-type-select')
    ? document.getElementById('doc-type-select').value
    : getPageDocType(state.selectedFile, state.selectedPage, file);

  fetch('/api/approve', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      filename: state.selectedFile,
      page_no: state.selectedPage,
      fields: fields,
      doc_type: docType
    })
  })
  .then(function(r) { return r.json(); })
  .then(function(res) {
    if (res.ok) {
      if (file.records && file.records[0]) file.records[0]._approved = 'true';
      var approvedTotal = state.files.filter(function(f) {
        return f.records && f.records.some(function(r) { return r._approved === 'true'; });
      }).length;
      document.getElementById('approved-count').textContent = '承認済: ' + approvedTotal;

      renderFileList();
      loadRightPanel(state.selectedFile, state.selectedPage, file);

      var nextFile = getNextPendingFile();
      if (nextFile) {
        setTimeout(function() { selectPage(nextFile.source_file, 1); }, 300);
      }
    }
  });
}

function skipRecord() {
  var nextFile = getNextPendingFile();
  if (nextFile) selectPage(nextFile.source_file, 1);
}

function runOcr() {
  // Placeholder: shows notification that OCR is not yet wired
  alert('OCR実行: このボタンは現在デモ表示です。サーバー側のOCRパイプラインを呼び出す実装が必要です。');
}

function getNextPendingFile() {
  var currentIdx = -1;
  for (var i = 0; i < state.files.length; i++) {
    if (state.files[i].source_file === state.selectedFile) { currentIdx = i; break; }
  }
  for (var j = currentIdx + 1; j < state.files.length; j++) {
    var f = state.files[j];
    if (!f.records || !f.records.some(function(r) { return r._approved === 'true'; })) {
      return f;
    }
  }
  return null;
}

// ── Utilities ──────────────────────────────────────────────────────────────
function escHtml(s) {
  return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
function escJs(s) {
  return String(s || '').replace(/\\/g,'\\\\').replace(/'/g,"\\'");
}
function escId(s) {
  return String(s || '').replace(/[^a-zA-Z0-9_-]/g, '_');
}
</script>
</body>
</html>
"""


# ─── HTTP Handler ─────────────────────────────────────────────────────────────

class ViewerHandler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        pass  # suppress access logs

    def send_json(self, data: dict | list, status: int = 200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_html(self, html: str):
        body = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_jpeg(self, img_bytes: bytes, cache_secs: int = 300):
        self.send_response(200)
        self.send_header("Content-Type", "image/jpeg")
        self.send_header("Content-Length", str(len(img_bytes)))
        self.send_header("Cache-Control", f"public, max-age={cache_secs}")
        self.end_headers()
        self.wfile.write(img_bytes)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)

        if path == "/":
            self.send_html(HTML_PAGE)

        elif path == "/api/files":
            self._handle_api_files()

        elif path.startswith("/api/image/"):
            # /api/image/<filename>/<page_no>?dpi=150
            rest = path[len("/api/image/"):]
            parts = rest.split("/")
            if len(parts) >= 2:
                filename = unquote(parts[0])
                try:
                    page_no = int(parts[1])
                except ValueError:
                    page_no = 1
                dpi = int(params.get("dpi", ["150"])[0])
                self._handle_image(filename, page_no, dpi)
            else:
                self.send_response(400)
                self.end_headers()

        elif path.startswith("/api/thumb/"):
            # /api/thumb/<filename>/<page_no>
            rest = path[len("/api/thumb/"):]
            parts = rest.split("/")
            if len(parts) >= 2:
                filename = unquote(parts[0])
                try:
                    page_no = int(parts[1])
                except ValueError:
                    page_no = 1
                self._handle_thumb(filename, page_no)
            else:
                self.send_response(400)
                self.end_headers()

        elif path.startswith("/api/ocr/"):
            filename = unquote(path[len("/api/ocr/"):])
            self._handle_ocr(filename)

        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)
        try:
            data = json.loads(body.decode("utf-8"))
        except Exception:
            data = {}

        if path == "/api/approve":
            self._handle_approve(data)
        elif path == "/api/tag":
            self._handle_tag(data)
        elif path == "/api/ocr-override":
            self._handle_ocr_override(data)
        else:
            self.send_response(404)
            self.end_headers()

    # ── GET /api/files ────────────────────────────────────────────────────────

    def _handle_api_files(self):
        # Group records by source_file
        groups: dict[str, list[dict]] = {}
        for rec in AppState.records:
            sf = rec.get("source_file", "")
            if sf not in groups:
                groups[sf] = []
            groups[sf].append(rec)

        # Also scan all PDFs in the directories (even those not in CSV)
        all_source_files = set(groups.keys())
        for directory in [SUCCESS_DIR, SKIP_DIR, INBOX_DIR]:
            if directory.exists():
                for p in sorted(directory.iterdir()):
                    if p.suffix.lower() == ".pdf":
                        all_source_files.add(p.name)

        files = []
        ok_count = 0
        skip_count = 0
        approved_count = 0

        for source_file in sorted(all_source_files):
            recs = groups.get(source_file, [])
            info = AppState.get_file_info(source_file)

            if recs:
                primary_rec = recs[0]
                parse_status = primary_rec.get("parse_status", "SKIP")
                is_approved = any(r.get("_approved") == "true" for r in recs)
                inferred_doc_type = infer_doc_type(primary_rec)
            else:
                primary_rec = {}
                parse_status = "SKIP"
                is_approved = False
                inferred_doc_type = "その他/不明"

            if parse_status == "OK":
                ok_count += 1
            else:
                skip_count += 1
            if is_approved:
                approved_count += 1

            # Build page_tags dict for this file
            page_tags: dict[int, str] = {}
            for page_no in range(1, info["page_count"] + 1):
                tag = AppState.page_tags.get((source_file, page_no))
                if tag:
                    page_tags[page_no] = tag
                else:
                    # First page gets the inferred type
                    if page_no == 1:
                        page_tags[page_no] = inferred_doc_type

            files.append({
                "source_file": source_file,
                "records": recs,
                "page_count": info["page_count"],
                "pdf_exists": info["exists"],
                "parse_status": parse_status,
                "doc_type": inferred_doc_type,
                "page_tags": page_tags,
            })

        self.send_json({
            "files": files,
            "stats": {
                "total": len(files),
                "ok": ok_count,
                "skip": skip_count,
                "approved": approved_count,
            }
        })

    # ── GET /api/image/<filename>/<page_no> ───────────────────────────────────

    def _handle_image(self, filename: str, page_no: int, dpi: int):
        pdf_path = find_pdf_path(filename)
        if not pdf_path or not pdf_path.exists():
            self.send_response(404)
            self.end_headers()
            return

        img_bytes = render_pdf_page(pdf_path, page_no, dpi=dpi)
        if img_bytes is None:
            self.send_response(500)
            self.end_headers()
            return

        self.send_jpeg(img_bytes)

    # ── GET /api/thumb/<filename>/<page_no> ───────────────────────────────────

    def _handle_thumb(self, filename: str, page_no: int):
        thumb = AppState.get_thumb(filename, page_no)
        if thumb is None:
            # Return a tiny placeholder SVG as fallback
            svg = b'<svg xmlns="http://www.w3.org/2000/svg" width="36" height="46"><rect width="36" height="46" fill="#e5e7eb"/></svg>'
            self.send_response(200)
            self.send_header("Content-Type", "image/svg+xml")
            self.send_header("Content-Length", str(len(svg)))
            self.end_headers()
            self.wfile.write(svg)
            return

        self.send_jpeg(thumb, cache_secs=3600)

    # ── GET /api/ocr/<filename> ───────────────────────────────────────────────

    def _handle_ocr(self, filename: str):
        # Start with staging CSV data
        ocr_data: dict = {}
        for rec in AppState.records:
            if rec.get("source_file") == filename:
                ocr_data = dict(rec)
                break

        # Apply overrides for the current page (default page 1 for file-level)
        page_no = 1
        overrides = AppState.ocr_overrides.get((filename, page_no), {})
        ocr_data.update(overrides)

        self.send_json(ocr_data if ocr_data else {})

    # ── POST /api/tag ─────────────────────────────────────────────────────────

    def _handle_tag(self, data: dict):
        filename = data.get("filename", "")
        page_no = int(data.get("page_no", 1))
        doc_type = data.get("doc_type", "その他/不明")

        if not filename:
            self.send_json({"ok": False, "error": "filename required"}, status=400)
            return

        save_page_tag(filename, page_no, doc_type)
        self.send_json({"ok": True})

    # ── POST /api/ocr-override ────────────────────────────────────────────────

    def _handle_ocr_override(self, data: dict):
        filename = data.get("filename", "")
        page_no = int(data.get("page_no", 1))
        fields = data.get("fields", {})

        if not filename or not fields:
            self.send_json({"ok": False, "error": "filename and fields required"}, status=400)
            return

        save_ocr_overrides(filename, page_no, fields)
        self.send_json({"ok": True})

    # ── POST /api/approve ─────────────────────────────────────────────────────

    def _handle_approve(self, data: dict):
        filename = data.get("filename", "")
        page_no = int(data.get("page_no", 1))
        fields = data.get("fields", {})
        doc_type = data.get("doc_type", "")

        if not filename:
            self.send_json({"ok": False, "error": "filename required"}, status=400)
            return

        # Save OCR overrides
        if fields:
            save_ocr_overrides(filename, page_no, fields)

        # Save page tag if provided
        if doc_type:
            save_page_tag(filename, page_no, doc_type)

        # Update in-memory approval status
        updated = False
        for rec in AppState.records:
            if rec.get("source_file") == filename:
                for key, val in fields.items():
                    rec[key] = val
                rec["_approved"] = "true"
                rec["_approved_at"] = datetime.now().isoformat()
                if doc_type:
                    rec["_doc_type"] = doc_type
                updated = True

        if updated:
            AppState.save_approved()
            company = fields.get("company_name_raw", filename)
            print(f"  [APPROVED] P{page_no} {company} → {AppState.approved_path.name}")

        self.send_json({"ok": True})


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="許可証ドキュメントビューワー（ページレベルタグ版）")
    parser.add_argument("--csv", type=str, help="staging CSVパス（省略時は最新）")
    parser.add_argument("--port", type=int, default=8080, help="ポート番号")
    args = parser.parse_args()

    # Load staging CSV (optional — viewer works even without CSV)
    records: list[dict] = []
    csv_path: Path | None = None
    try:
        csv_path = Path(args.csv) if args.csv else find_latest_staging_csv()
        records = load_staging_csv(csv_path)
    except FileNotFoundError:
        print("[INFO] staging CSVが見つかりません。PDF一覧のみ表示します。")

    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    approved_path = STAGING_DIR / f"approved_{csv_path.stem}.csv" if csv_path else STAGING_DIR / "approved_manual.csv"

    print("=" * 60)
    print("  許可証ドキュメントビューワー（ページレベルタグ版）")
    print("=" * 60)
    if csv_path:
        print(f"  staging CSV : {csv_path}")
    print(f"  全レコード  : {len(records)} 件")
    ok_count = sum(1 for r in records if r.get("parse_status") == "OK")
    skip_count = len(records) - ok_count
    print(f"  OK件数      : {ok_count} 件")
    print(f"  SKIP件数    : {skip_count} 件")
    print(f"  page_tags   : {PAGE_TAGS_CSV}")
    print(f"  ocr_overrides: {OCR_OVERRIDES_CSV}")
    print(f"  承認CSV出力 : {approved_path}")
    print(f"  URL         : http://127.0.0.1:{args.port}")
    print()

    if not FITZ_AVAILABLE:
        print("[WARNING] PyMuPDF未インストール: PDFレンダリング無効")
        print("          pip install pymupdf でインストールしてください")
        print()

    # Initialize shared state
    AppState.records = records
    AppState.csv_path = csv_path or Path()
    AppState.approved_path = approved_path
    AppState.page_tags = load_page_tags()
    AppState.ocr_overrides = load_ocr_overrides()

    # Pre-cache file info (page counts) — thumbnails are lazy-loaded per request
    unique_files: list[str] = list(dict.fromkeys(r.get("source_file", "") for r in records))

    # Also discover PDFs from directories
    for directory in [SUCCESS_DIR, SKIP_DIR, INBOX_DIR]:
        if directory.exists():
            for p in sorted(directory.iterdir()):
                if p.suffix.lower() == ".pdf" and p.name not in unique_files:
                    unique_files.append(p.name)

    print(f"  ファイルキャッシュ構築中 ({len(unique_files)} ファイル)...")
    found = 0
    for sf in unique_files:
        info = AppState.get_file_info(sf)
        if info["exists"]:
            found += 1
    print(f"    → PDF存在: {found} / {len(unique_files)}")
    print()
    print("  Ctrl+C で終了")
    print()

    server = HTTPServer(("127.0.0.1", args.port), ViewerHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        approved_total = sum(1 for r in records if r.get("_approved") == "true")
        print(f"\n  承認済み: {approved_total}/{len(records)} 件")
        if approved_total > 0:
            print(f"  承認CSV : {approved_path}")


if __name__ == "__main__":
    main()
