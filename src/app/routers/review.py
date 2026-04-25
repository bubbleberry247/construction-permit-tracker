"""
分類レビューキュー用API。
低confidence または その他/不明 ページを優先度順に返す。

判定ロジック:
  A. doc_type_name IN ('その他/不明','その他') → 要確認
  B. confidence IS NOT NULL AND confidence < 0.8 → 要確認
  C. confidence IS NULL:
      - 会社に手動修正履歴（viewer_correction等）あり → 除外（手動チェック済と推定）
      - 履歴なし → 要確認
"""
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query

from app.database import get_db

router = APIRouter(prefix="/api/review", tags=["review"])

MANUAL_CONFIRM_BY = (
    "viewer_correction",
    "manual_review_v2",
    "manual_review",
    "web_viewer",
    "user_manual",
    "manual_visual",
)
_MANUAL_SET_SQL = ",".join(f"'{x}'" for x in MANUAL_CONFIRM_BY)

# 要確認判定 WHERE 句（除外節）
REVIEW_WHERE = (
    "(p.doc_type_name IN ('その他/不明', 'その他') "
    "OR (p.confidence IS NOT NULL AND p.confidence < 0.8) "
    "OR (p.confidence IS NULL AND p.company_id NOT IN "
    f"    (SELECT DISTINCT company_id FROM field_reviews WHERE confirmed_by IN ({_MANUAL_SET_SQL}))))"
)


@router.get("/queue")
def review_queue(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    include_fixed: bool = Query(False, description="True で修正済みも含む"),
):
    conn = get_db()
    try:
        where_clause = "" if include_fixed else f"WHERE {REVIEW_WHERE}"

        total = conn.execute(
            f"SELECT COUNT(*) FROM pages p {where_clause}"
        ).fetchone()[0]

        rows = conn.execute(
            f"SELECT p.page_id, p.company_id, c.official_name, p.file_name, p.page_no, "
            f"p.doc_type_name, p.confidence "
            f"FROM pages p JOIN companies c ON c.company_id = p.company_id "
            f"{where_clause} "
            "ORDER BY "
            "CASE WHEN p.doc_type_name='その他/不明' THEN 0 "
            "     WHEN p.doc_type_name='その他' THEN 1 "
            "     WHEN p.confidence IS NULL THEN 2 "
            "     WHEN p.confidence < 0.5 THEN 3 "
            "     WHEN p.confidence < 0.8 THEN 4 "
            "     ELSE 5 END, "
            "p.company_id, p.file_name, p.page_no "
            "LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()

        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "items": [dict(r) for r in rows],
        }
    finally:
        conn.close()


@router.get("/counts")
def review_counts():
    """残件サマリ（ダッシュボード表示用）"""
    conn = get_db()
    try:
        # 手動修正あり会社
        manual_cos_cnt = conn.execute(
            f"SELECT COUNT(DISTINCT company_id) FROM field_reviews WHERE confirmed_by IN ({_MANUAL_SET_SQL})"
        ).fetchone()[0]

        other = conn.execute(
            "SELECT COUNT(*) FROM pages WHERE doc_type_name IN ('その他/不明', 'その他')"
        ).fetchone()[0]
        conf_low = conn.execute(
            "SELECT COUNT(*) FROM pages WHERE confidence IS NOT NULL AND confidence < 0.8 "
            "AND doc_type_name NOT IN ('その他/不明','その他')"
        ).fetchone()[0]
        null_excluded = conn.execute(
            f"SELECT COUNT(*) FROM pages WHERE confidence IS NULL "
            "AND doc_type_name NOT IN ('その他/不明','その他') "
            f"AND company_id IN (SELECT DISTINCT company_id FROM field_reviews WHERE confirmed_by IN ({_MANUAL_SET_SQL}))"
        ).fetchone()[0]
        null_requires = conn.execute(
            f"SELECT COUNT(*) FROM pages WHERE confidence IS NULL "
            "AND doc_type_name NOT IN ('その他/不明','その他') "
            f"AND company_id NOT IN (SELECT DISTINCT company_id FROM field_reviews WHERE confirmed_by IN ({_MANUAL_SET_SQL}))"
        ).fetchone()[0]
        total = conn.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
        return {
            "total_pages": total,
            "manual_reviewed_companies": manual_cos_cnt,
            "unknown": other,
            "confidence_low": conf_low,
            "confidence_null_excluded_manual": null_excluded,
            "confidence_null_requires_review": null_requires,
            "review_total": other + conf_low + null_requires,
        }
    finally:
        conn.close()


@router.get("/ocr_mismatch_queue")
def ocr_mismatch_queue(limit: int = Query(500, ge=1, le=2000)):
    """GPT-5.x audit OCR と現状 doc_type が不一致なページのキュー。

    ocr_runs.run_type='reocr_audit_only' の最新レコードと
    pages.doc_type_name を比較し、不一致のみ返す。
    field_reviews.confirmed_by='ocr_mismatch_confirmed' のページは除外。
    confidence 降順で並べる。
    """
    conn = get_db()
    try:
        rows = conn.execute(
            """
            SELECT p.page_id, p.company_id, c.official_name,
                   p.file_name, p.page_no,
                   p.doc_type_name AS current_doc_type,
                   r.raw_response, r.created_at AS audit_at
            FROM ocr_runs r
            JOIN pages p ON p.page_id = r.page_id
            JOIN companies c ON c.company_id = p.company_id
            WHERE r.run_type = 'reocr_audit_only'
              AND r.run_id = (
                SELECT MAX(run_id) FROM ocr_runs
                WHERE page_id = r.page_id AND run_type = 'reocr_audit_only'
              )
              AND NOT EXISTS (
                SELECT 1 FROM field_reviews fr
                WHERE fr.confirmed_by = 'ocr_mismatch_confirmed'
                  AND fr.field_name = ('ocr_mismatch:' || p.page_id)
              )
            """
        ).fetchall()
        items = []
        import json as _json
        for row in rows:
            try:
                pr = _json.loads(row["raw_response"])
            except Exception:
                continue
            ocr_doc_type = pr.get("type_name") or ""
            ocr_conf = float(pr.get("confidence", 0))
            ocr_rotation = int(pr.get("rotation", 0))
            if not ocr_doc_type or ocr_doc_type == row["current_doc_type"]:
                continue
            items.append({
                "page_id": row["page_id"],
                "company_id": row["company_id"],
                "official_name": row["official_name"],
                "file_name": row["file_name"],
                "page_no": row["page_no"],
                "current_doc_type": row["current_doc_type"],
                "ocr_doc_type": ocr_doc_type,
                "ocr_confidence": ocr_conf,
                "ocr_rotation": ocr_rotation,
                "audit_at": row["audit_at"],
            })
        items.sort(key=lambda x: -x["ocr_confidence"])
        return {"total": len(items), "limit": limit, "items": items[:limit]}
    finally:
        conn.close()


@router.post("/confirm_ocr_mismatch/{page_id}")
def confirm_ocr_mismatch(page_id: int):
    """OCR 不一致ページを「人手分類が正しい」として確定。次回キューから除外。"""
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT page_id, company_id FROM pages WHERE page_id=?", (page_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, f"page_id {page_id} not found")
        # 既に確定済みかチェック
        existing = conn.execute(
            "SELECT review_id FROM field_reviews "
            "WHERE confirmed_by='ocr_mismatch_confirmed' AND field_name=?",
            (f"ocr_mismatch:{page_id}",),
        ).fetchone()
        if existing:
            return {"status": "already_confirmed", "page_id": page_id}
        now = datetime.now().isoformat()
        conn.execute(
            "INSERT INTO field_reviews (company_id, field_name, confirmed_value, confirmed_by, reviewed_at, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (row["company_id"], f"ocr_mismatch:{page_id}", "manual_kept",
             "ocr_mismatch_confirmed", now, now),
        )
        conn.commit()
        return {"status": "confirmed", "page_id": page_id}
    finally:
        conn.close()


@router.get("/sample_null_manual")
def sample_null_with_manual_history(limit: int = Query(10, ge=1, le=50)):
    """「手動修正履歴あり会社」×「confidence NULL」のページをサンプル出力（B案検証用）"""
    conn = get_db()
    try:
        rows = conn.execute(
            f"SELECT p.page_id, p.company_id, c.official_name, p.file_name, p.page_no, p.doc_type_name "
            f"FROM pages p JOIN companies c ON c.company_id = p.company_id "
            "WHERE p.confidence IS NULL "
            "AND p.doc_type_name NOT IN ('その他/不明','その他') "
            f"AND p.company_id IN (SELECT DISTINCT company_id FROM field_reviews WHERE confirmed_by IN ({_MANUAL_SET_SQL})) "
            "ORDER BY random() LIMIT ?",
            (limit,),
        ).fetchall()
        return {"items": [dict(r) for r in rows]}
    finally:
        conn.close()
