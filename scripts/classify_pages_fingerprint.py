"""
Phase 3: フィンガープリントベースのページ分類器（shadow mode）。

設計鉄則:
  1. pages テーブルは触らない（書き込み先は classification_candidates のみ）
  2. 手動ラベル (web_viewer/user_visual/masaru_manual) は manual_locked=1 でマーク
  3. OCR テキストは字間スペースを正規化してから照合
  4. score = heading + required + support + regex - negative
  5. confidence = clip( (score - min_score) / divisor + floor, floor, ceiling )
  6. 全ページを評価する（manual も含めて gold set 評価で precision/recall を測れるように）

Usage:
  python scripts/classify_pages_fingerprint.py --shadow
  python scripts/classify_pages_fingerprint.py --shadow --company-ids C0124,C0111
  python scripts/classify_pages_fingerprint.py --shadow --limit 200
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import pymupdf as fitz
import yaml

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
OCR_CACHE = PROJECT / "data" / "ocr_cache"
ORIGINALS = PROJECT / "data" / "originals"
FP_YAML = PROJECT / "config" / "doc_fingerprints_v1.yaml"
sys.stdout.reconfigure(encoding="utf-8")

CLASSIFIER_VERSION = "fingerprint_v1"
HEADING_TOP_CHARS = 200
HEADING_SCORE = 3.0
REQUIRED_KW_SCORE = 1.5
SUPPORT_KW_SCORE = 0.5
NEGATIVE_KW_SCORE = -1.0


# ---------------------------------------------------------------------------
# テキスト正規化
# ---------------------------------------------------------------------------
_JP_CHAR = r'[ぁ-んァ-ヴー一-龯々]'

def normalize_ocr_text(t: str) -> str:
    """日本語文字間のスペースを除去 + 連続スペース圧縮。"""
    if not t:
        return ""
    # 日本語文字 + 空白 + 日本語文字 → 空白除去
    t = re.sub(rf'({_JP_CHAR})\s+(?={_JP_CHAR})', r'\1', t)
    # 数字内の空白 (例 "1, 234") は数字のセパレータも保持: 「\d , \d」 → 「\d,\d」
    t = re.sub(r'(\d)\s*,\s*(\d)', r'\1,\2', t)
    # 連続空白 → 単一
    t = re.sub(r'[ \t]+', ' ', t)
    return t


# ---------------------------------------------------------------------------
# OCR テキスト取得 (PyMuPDF -> OCR cache fallback)
# ---------------------------------------------------------------------------
_pdf_hash_cache: dict[str, str] = {}


def find_pdf(file_name: str, cid: str) -> Path | None:
    if file_name.lower().endswith(".xlsx"):
        # xlsx は事前変換済の同名 .pdf を探す
        alt = file_name[:-5] + ".pdf"
        for c in (ORIGINALS).glob(f"{cid}_*"):
            for p in c.rglob(alt):
                if p.is_file():
                    return p
    for c in (ORIGINALS).glob(f"{cid}_*"):
        for p in c.rglob(file_name):
            if p.is_file():
                return p
        # サブパス指定だった場合
        for p in c.rglob(f"*{file_name}"):
            if p.is_file():
                return p
    return None


def pdf_file_sha256(pdf: Path) -> str:
    key = str(pdf.resolve())
    if key in _pdf_hash_cache:
        return _pdf_hash_cache[key]
    h = hashlib.sha256()
    with pdf.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    _pdf_hash_cache[key] = h.hexdigest()
    return _pdf_hash_cache[key]


def get_ocr_text(pdf: Path, page_no: int) -> str:
    """PyMuPDF テキスト → 不足時 OCR キャッシュを参照。"""
    text = ""
    try:
        doc = fitz.open(pdf)
        if page_no <= len(doc):
            text = (doc[page_no - 1].get_text() or "").strip()
        doc.close()
    except Exception:
        text = ""

    if len(text) >= 30:
        return text

    # OCR cache 1: per-page (page_<filehash>_p<pno>.json) - newer
    try:
        h = pdf_file_sha256(pdf)
        per_page = OCR_CACHE / f"page_{h}_p{page_no}.json"
        if per_page.exists():
            d = json.loads(per_page.read_text(encoding="utf-8"))
            cached = d.get("text", "") or ""
            if len(cached) > len(text):
                return cached
    except Exception:
        pass

    # OCR cache 2: full-doc list ([h].json) - older (Azure DI batch)
    try:
        h = pdf_file_sha256(pdf)
        full = OCR_CACHE / f"{h}.json"
        if full.exists():
            arr = json.loads(full.read_text(encoding="utf-8"))
            if isinstance(arr, list) and 1 <= page_no <= len(arr):
                cached = arr[page_no - 1] or ""
                if len(cached) > len(text):
                    return cached
    except Exception:
        pass

    return text


# ---------------------------------------------------------------------------
# Fingerprint scoring
# ---------------------------------------------------------------------------
class Fingerprint:
    def __init__(self, label: str, cfg: dict):
        self.label = label
        self.headings = cfg.get("headings", []) or []
        self.required = cfg.get("required_keywords", []) or []
        self.support = cfg.get("support_keywords", []) or []
        self.negative = cfg.get("negative_keywords", []) or []
        self.regex = {
            name: (re.compile(pat[0]), float(pat[1]))
            for name, pat in (cfg.get("regex_patterns") or {}).items()
        }
        self.min_score = float(cfg.get("min_score", 2.5))
        self.divisor = float(cfg.get("confidence_divisor", 5.0))
        self.floor = float(cfg.get("confidence_floor", 0.80))
        self.ceiling = float(cfg.get("confidence_ceiling", 0.95))
        self.extra = cfg.get("extra_checks", {}) or {}

    def score(self, t: str, top: str) -> tuple[float, dict[str, Any]]:
        s = 0.0
        ev: dict[str, Any] = {"heading": [], "required": [], "support": [], "regex": [], "negative": [], "extra": {}}

        # heading - top で照合
        for h in self.headings:
            if h in top:
                s += HEADING_SCORE
                ev["heading"].append(h)
                break  # 1ヒットで十分

        # required keywords
        for k in self.required:
            if k in t:
                s += REQUIRED_KW_SCORE
                ev["required"].append(k)

        # support keywords
        for k in self.support:
            if k in t:
                s += SUPPORT_KW_SCORE
                ev["support"].append(k)

        # regex patterns
        for name, (pat, score) in self.regex.items():
            n = len(pat.findall(t))
            if n > 0:
                # patterns where score is small (< 0.5), allow stacking up to 5
                add = min(score * n, score * 5) if score < 0.5 else score
                s += add
                ev["regex"].append({"name": name, "hits": n, "score": round(add, 2)})

        # negative keywords
        for k in self.negative:
            if k in t:
                s += NEGATIVE_KW_SCORE
                ev["negative"].append(k)

        # extra checks (boost or block)
        if "amount_count_min" in self.extra:
            n = len(re.findall(r'\d{1,3}(?:,\d{3}){2,}', t))
            ev["extra"]["amount_count"] = n
            if n < self.extra["amount_count_min"]:
                s -= 1.5
        if "birthdate_count_min" in self.extra:
            n = len(re.findall(r'\d{4}/\d{1,2}/\d{1,2}', t)) + len(re.findall(r'(昭和|平成)\d+年\d+月\d+日', t))
            ev["extra"]["birthdate_count"] = n
            if n < self.extra["birthdate_count_min"]:
                s -= 1.5
        if "company_count_min" in self.extra:
            n = len(re.findall(r'(株式会社|有限会社|\(株\)|㈱|\(有\)|㈲)', t))
            ev["extra"]["company_count"] = n
            if n < self.extra["company_count_min"]:
                s -= 1.5

        return s, ev

    def confidence(self, score: float) -> float:
        if score < self.min_score:
            return 0.0
        c = (score - self.min_score) / self.divisor + self.floor
        return max(self.floor, min(self.ceiling, c))


def load_fingerprints() -> dict[str, Fingerprint]:
    cfg = yaml.safe_load(FP_YAML.read_text(encoding="utf-8"))
    return {
        label: Fingerprint(label, body)
        for label, body in (cfg.get("documents") or {}).items()
    }


# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------
def classify(text: str, fps: dict[str, Fingerprint]) -> tuple[str, float, str, float, dict]:
    if not text or len(text) < 10:
        return ("その他/不明", 0.30, "", 0.0, {"reason": "text_too_short", "len": len(text), "text_quality": "no_text"})

    norm = normalize_ocr_text(text)
    top = norm[:HEADING_TOP_CHARS]

    # GPT-5.5 推奨 text_quality 階層
    norm_no_ws = re.sub(r'\s+', '', norm)
    L = len(norm_no_ws)
    if L < 50:
        text_quality = "low_text"
    elif L < 150:
        text_quality = "weak_text"
    else:
        text_quality = "normal"

    scored: list[tuple[str, float, float, dict]] = []
    for label, fp in fps.items():
        s, ev = fp.score(norm, top)
        c = fp.confidence(s)
        if s >= fp.min_score:
            scored.append((label, c, s, ev))

    if not scored:
        return ("その他/不明", 0.30, "", 0.0, {"reason": "no_class_above_min_score", "text_quality": text_quality, "len": L})

    # confidence 降順
    scored.sort(key=lambda x: (x[1], x[2]), reverse=True)
    top1 = scored[0]
    top2 = scored[1] if len(scored) > 1 else (None, 0.0, 0.0, {})

    return (
        top1[0], top1[1],
        top2[0] or "", top2[1],
        {"top_score": round(top1[2], 2),
         "second_score": round(top2[2], 2) if top2[0] else 0,
         "text_quality": text_quality,
         "text_len_no_ws": L,
         "evidence": top1[3]}
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def is_manual_locked(conn, page_id: int) -> bool:
    r = conn.execute(
        """SELECT 1 FROM page_doc_type_history
           WHERE page_id=? AND confirmed_by IN ('web_viewer','user_visual','masaru_manual')
           LIMIT 1""",
        (page_id,)
    ).fetchone()
    return r is not None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--shadow", action="store_true", help="shadow mode: pages を触らずに classification_candidates のみ書く")
    ap.add_argument("--company-ids", help="C0001,C0002 ... カンマ区切り")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--run-id", help="既存 run_id 上書き (デバッグ)")
    args = ap.parse_args()

    if not args.shadow:
        print("ERROR: --shadow を指定してください（promote は別スクリプトで行います）", file=sys.stderr)
        sys.exit(1)

    fps = load_fingerprints()
    print(f"[init] fingerprints loaded: {list(fps.keys())}", file=sys.stderr)

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    where = []
    params: list = []
    if args.company_ids:
        cids = [c.strip() for c in args.company_ids.split(",") if c.strip()]
        if cids:
            where.append(f"company_id IN ({','.join('?' for _ in cids)})")
            params.extend(cids)

    q = "SELECT page_id, company_id, file_name, page_no, doc_type_name FROM pages"
    if where:
        q += " WHERE " + " AND ".join(where)
    q += " ORDER BY company_id, file_name, page_no"
    if args.limit:
        q += f" LIMIT {args.limit}"

    pages = list(conn.execute(q, params).fetchall())
    print(f"[init] target pages: {len(pages)}", file=sys.stderr)

    run_id = args.run_id or f"fp_v1_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    conn.execute("BEGIN")
    conn.execute(
        """INSERT OR REPLACE INTO classification_runs
           (run_id, classifier_version, fingerprint_version, mode, pages_processed)
           VALUES (?, ?, ?, 'shadow', 0)""",
        (run_id, CLASSIFIER_VERSION, "v1")
    )

    pdf_cache: dict[str, Path | None] = {}

    n_high = 0
    n_changed = 0
    n_no_text = 0
    by_pred: dict[str, int] = {}

    for i, p in enumerate(pages, 1):
        pid = p["page_id"]
        cid = p["company_id"]
        fn = p["file_name"]
        pno = p["page_no"]
        cur_label = p["doc_type_name"] or ""

        # PDF 取得
        cache_key = f"{cid}/{fn}"
        if cache_key in pdf_cache:
            pdf = pdf_cache[cache_key]
        else:
            pdf = find_pdf(fn, cid)
            pdf_cache[cache_key] = pdf

        if pdf is None:
            text = ""
        else:
            text = get_ocr_text(pdf, pno)

        if not text:
            n_no_text += 1

        pred, conf, second, second_conf, meta = classify(text, fps)
        by_pred[pred] = by_pred.get(pred, 0) + 1

        manual_locked = 1 if is_manual_locked(conn, pid) else 0
        would_change = 0 if pred == cur_label else 1

        if conf >= 0.85:
            n_high += 1
        if would_change and pred != "その他/不明":
            n_changed += 1

        conn.execute(
            """INSERT INTO classification_candidates
               (page_id, run_id, classifier_version, predicted_label, confidence,
                second_label, second_confidence, evidence_json,
                previous_effective_label, would_change, manual_locked)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (pid, run_id, CLASSIFIER_VERSION, pred, round(conf, 4),
             second or None, round(second_conf, 4) if second else None,
             json.dumps(meta, ensure_ascii=False),
             cur_label, would_change, manual_locked)
        )

        if i % 500 == 0:
            print(f"  進捗 {i}/{len(pages)} (high={n_high}, changed={n_changed})", file=sys.stderr, flush=True)

    conn.execute(
        "UPDATE classification_runs SET pages_processed=?, pages_high_conf=?, pages_changed=? WHERE run_id=?",
        (len(pages), n_high, n_changed, run_id)
    )
    conn.commit()
    print()
    print(f"=== run_id: {run_id} ===")
    print(f"  pages: {len(pages)}")
    print(f"  high confidence (>=0.85): {n_high}")
    print(f"  would change: {n_changed}")
    print(f"  no text/extract fail: {n_no_text}")
    print(f"\n=== 予測分布 ===")
    for label, n in sorted(by_pred.items(), key=lambda x: -x[1]):
        print(f"  {label:25s} {n}")
    print(f"\n次のステップ:")
    print(f"  python scripts/evaluate_classifier_gold.py --run-id {run_id}")


if __name__ == "__main__":
    main()
