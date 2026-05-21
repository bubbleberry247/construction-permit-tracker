from fastapi import APIRouter, Request

from app.database import get_db

router = APIRouter(tags=["dashboard"])

# (表示名, 必須フラグ) — 会社案内は任意（あれば）
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

REQUIRED_DOC_TYPES = [
    "取引申請書",
    "建設業許可証",
    "決算書",
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

    # 客先 (TIC) 確認状況: client_review_status の最新値を会社単位で取得
    # confirmed_value は 'status|date|note' 形式 (status enum: confirmed / needs_action / closed)
    # 優先順位 (新しいレビューほど優先): closed > needs_action > confirmed
    STATUS_PRIORITY = {'confirmed': 1, 'needs_action': 2, 'closed': 3}
    BADGE_MAP = {
        'confirmed':    ('badge-client-confirmed',  '客先 確認済'),
        'needs_action': ('badge-client-action',     '客先 要対応'),
        'closed':       ('badge-client-closed',     '客先 対応完了'),
    }
    client_status_map: dict[str, dict] = {}
    for r in conn.execute(
        "SELECT company_id, confirmed_value, reviewed_at "
        "FROM field_reviews "
        "WHERE field_name = 'client_review_status' "
        "ORDER BY reviewed_at"
    ).fetchall():
        cid = r["company_id"]
        val = r["confirmed_value"] or ''
        status = val.split('|', 1)[0] if '|' in val else val
        if status not in BADGE_MAP:
            continue
        cur = client_status_map.get(cid)
        new_p = STATUS_PRIORITY.get(status, 0)
        if cur and STATUS_PRIORITY.get(cur['status'], 0) >= new_p:
            continue
        # note は status|date|note の 3 番目以降
        note = val.split('|', 2)[-1] if val.count('|') >= 1 else ''
        client_status_map[cid] = {
            'status': status,
            'badge_class': BADGE_MAP[status][0],
            'badge_label': BADGE_MAP[status][1],
            'note': note,
            'at': r['reviewed_at'],
        }

    # ページ分類 (is_active=1 のみ; quarantine された重複を除外)
    page_counts = conn.execute(
        "SELECT company_id, doc_type_name, COUNT(*) AS cnt "
        "FROM pages WHERE is_active = 1 GROUP BY company_id, doc_type_name"
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
        for dt in DOC_TYPES:
            found = dt in company_types
            cells.append({"found": found, "received": is_received})
        # 不足判定は必須書類のみ（会社案内は任意なので除外）
        missing = [dt for dt in REQUIRED_DOC_TYPES if dt not in company_types]
        client = client_status_map.get(cid)
        grid.append({
            "company_id": cid,
            "official_name": comp["official_name"],
            "cells": cells,
            "is_received": is_received,
            "missing": missing if is_received else [],
            "missing_count": len(missing) if is_received else 0,
            "complete": len(missing) == 0 and is_received,
            "client_badge_class": client['badge_class'] if client else None,
            "client_badge_label": client['badge_label'] if client else None,
            "client_note":        client['note']        if client else None,
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
