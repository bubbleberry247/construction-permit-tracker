from fastapi import APIRouter, HTTPException, Query

from app.database import get_db

router = APIRouter(prefix="/api/companies", tags=["companies"])


@router.get("/nav")
def nav_companies(filter: str = Query("all")):
    """Return lightweight company list for sidebar navigation with filter support."""
    conn = get_db()
    try:
        # All active companies sorted by official_name
        companies = conn.execute(
            "SELECT company_id, official_name FROM companies "
            "WHERE status = 'ACTIVE' ORDER BY official_name"
        ).fetchall()

        if filter == "all":
            result = [{"company_id": r["company_id"], "official_name": r["official_name"]} for r in companies]
            return {"companies": result, "total": len(result), "filter": filter}

        # Received company IDs (resolved receipt_events)
        received_ids = set(
            r[0] for r in conn.execute(
                "SELECT DISTINCT company_id FROM receipt_events "
                "WHERE resolve_status = 'resolved' AND company_id IS NOT NULL"
            ).fetchall()
        )

        # Doc types per company
        page_counts = conn.execute(
            "SELECT company_id, doc_type_name, COUNT(*) AS cnt "
            "FROM pages GROUP BY company_id, doc_type_name"
        ).fetchall()

        doc_map: dict[str, set[str]] = {}
        for row in page_counts:
            cid = row["company_id"]
            dtn = row["doc_type_name"] or ""
            if cid not in doc_map:
                doc_map[cid] = set()
            doc_map[cid].add(dtn)

        REQUIRED_DOC_TYPES = [
            "取引申請書", "建設業許可証", "決算書",
            "工事経歴書", "取引先一覧表", "労働安全衛生誓約書",
            "資格略字一覧", "労働者名簿",
        ]

        result = []
        for comp in companies:
            cid = comp["company_id"]
            is_received = cid in received_ids
            company_types = doc_map.get(cid, set())
            missing_count = sum(1 for dt in REQUIRED_DOC_TYPES if dt not in company_types)
            is_complete = is_received and missing_count == 0

            include = False
            if filter == "received":
                include = is_received
            elif filter == "missing":
                include = is_received and missing_count > 0
            elif filter == "complete":
                include = is_complete
            elif filter == "unreceived":
                include = not is_received

            if include:
                result.append({"company_id": cid, "official_name": comp["official_name"]})

        return {"companies": result, "total": len(result), "filter": filter}
    finally:
        conn.close()


@router.get("")
def list_companies():
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT company_id, official_name, status FROM companies "
            "WHERE status = 'ACTIVE' ORDER BY official_name"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@router.get("/{company_id}")
def get_company(company_id: str):
    conn = get_db()
    try:
        # Company
        comp = conn.execute(
            "SELECT company_id, official_name, status FROM companies WHERE company_id = ?",
            (company_id,),
        ).fetchone()
        if comp is None:
            raise HTTPException(404, f"Company not found: {company_id}")

        # Pages
        pages = conn.execute(
            "SELECT page_id, file_name, page_no, doc_type_name, doc_type_id, "
            "rotation, confidence FROM pages WHERE company_id = ? "
            "ORDER BY file_name, page_no",
            (company_id,),
        ).fetchall()

        # Permits
        permits = conn.execute(
            "SELECT p.permit_id, p.permit_number, p.permit_authority, "
            "p.permit_category, p.permit_year, p.issue_date, p.expiry_date, "
            "GROUP_CONCAT(pt.trade_name, ', ') AS trade_names "
            "FROM permits p "
            "LEFT JOIN permit_trades pt ON pt.permit_id = p.permit_id "
            "WHERE p.company_id = ? AND p.current_flag = 1 "
            "GROUP BY p.permit_id",
            (company_id,),
        ).fetchall()

        # Mail received?
        received = conn.execute(
            "SELECT 1 FROM receipt_events "
            "WHERE company_id = ? AND resolve_status = 'resolved' LIMIT 1",
            (company_id,),
        ).fetchone()

        return {
            "company": dict(comp),
            "pages": [dict(p) for p in pages],
            "permits": [dict(p) for p in permits],
            "mail_received": received is not None,
        }
    finally:
        conn.close()
