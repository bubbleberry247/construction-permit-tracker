from fastapi import APIRouter, Request

from app.database import get_db

router = APIRouter(tags=["dashboard"])

DOC_TYPES = [
    "取引申請書",
    "建設業許可証",
    "決算書",
    "会社案内",
    "工事経歴書",
    "取引先一覧表",
    "労働安全衛生誓約書",
    "資格略字一覧",
    "労働者名簿",
]


def _build_grid(conn):
    """Build company × doc_type grid with stats."""
    companies = conn.execute(
        "SELECT company_id, official_name FROM companies "
        "WHERE status = 'ACTIVE' ORDER BY official_name"
    ).fetchall()

    # 受信済み会社
    received_ids = set(
        r[0] for r in conn.execute(
            "SELECT DISTINCT company_id FROM receipt_events "
            "WHERE resolve_status = 'resolved' AND company_id IS NOT NULL"
        ).fetchall()
    )

    # ページ分類
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

    grid = []
    for comp in companies:
        cid = comp["company_id"]
        is_received = cid in received_ids
        company_types = doc_map.get(cid, set())
        cells = []
        missing = []
        for dt in DOC_TYPES:
            found = dt in company_types
            cells.append({"found": found, "received": is_received})
            if is_received and not found:
                missing.append(dt)
        grid.append({
            "company_id": cid,
            "official_name": comp["official_name"],
            "cells": cells,
            "is_received": is_received,
            "missing": missing,
            "missing_count": len(missing),
            "complete": len(missing) == 0 and is_received,
        })

    # サマリ統計
    total_active = len(companies)
    total_received = sum(1 for r in grid if r["is_received"])
    total_unreceived = total_active - total_received
    total_complete = sum(1 for r in grid if r["complete"])

    # 書類種別別充足率（受信済みベース）
    doc_stats = []
    for dt in DOC_TYPES:
        has = sum(1 for r in grid if r["is_received"] and dt in doc_map.get(r["company_id"], set()))
        pct = has / total_received * 100 if total_received else 0
        doc_stats.append({"name": dt, "count": has, "total": total_received, "pct": round(pct, 1)})

    return {
        "grid": grid,
        "doc_types": DOC_TYPES,
        "total_active": total_active,
        "total_received": total_received,
        "total_unreceived": total_unreceived,
        "total_complete": total_complete,
        "doc_stats": doc_stats,
    }


@router.get("/dashboard")
def dashboard(request: Request, show: str = "all"):
    templates = request.app.state.templates
    conn = get_db()
    try:
        data = _build_grid(conn)

        # フィルタ
        if show == "missing":
            data["grid"] = [r for r in data["grid"] if r["is_received"] and r["missing_count"] > 0]
        elif show == "complete":
            data["grid"] = [r for r in data["grid"] if r["complete"]]
        elif show == "unreceived":
            data["grid"] = [r for r in data["grid"] if not r["is_received"]]
        elif show == "received":
            data["grid"] = [r for r in data["grid"] if r["is_received"]]

        return templates.TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "show": show,
                **data,
            },
        )
    finally:
        conn.close()


@router.get("/viewer")
def viewer(request: Request):
    templates = request.app.state.templates
    initial_filter = request.query_params.get("filter", "all")
    return templates.TemplateResponse(
        "viewer.html",
        {
            "request": request,
            "initial_company_id": "",
            "initial_filter": initial_filter,
        },
    )


@router.get("/viewer/{company_id}")
def viewer_company(request: Request, company_id: str):
    templates = request.app.state.templates
    initial_filter = request.query_params.get("filter", "all")
    return templates.TemplateResponse(
        "viewer.html",
        {
            "request": request,
            "initial_company_id": company_id,
            "initial_filter": initial_filter,
        },
    )
