import uuid
from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.database import get_db

router = APIRouter(prefix="/api/pages", tags=["pages"])


class PageUpdate(BaseModel):
    doc_type_name: str | None = None
    doc_type_secondary: str | None = None
    rotation: int | None = None


@router.get("/{company_id}")
def list_pages(company_id: str):
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT page_id, file_name, page_no, doc_type_name, doc_type_id, "
            "rotation, confidence FROM pages WHERE company_id = ? "
            "ORDER BY file_name, page_no",
            (company_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@router.patch("/{page_id}")
def update_page(page_id: int, body: PageUpdate):
    conn = get_db()
    try:
        page = conn.execute(
            "SELECT page_id, company_id, doc_type_name, rotation FROM pages "
            "WHERE page_id = ?",
            (page_id,),
        ).fetchone()
        if page is None:
            raise HTTPException(404, f"Page not found: {page_id}")

        company_id = page["company_id"]

        updates = []
        params = []

        if body.doc_type_name is not None:
            updates.append("doc_type_name = ?")
            params.append(body.doc_type_name)
            conn.execute(
                "INSERT INTO field_reviews (company_id, field_name, confirmed_value, confirmed_by) "
                "VALUES (?, 'doc_type_name', ?, 'web_viewer')",
                (company_id, body.doc_type_name),
            )
            old_doc = page["doc_type_name"]
            if old_doc != body.doc_type_name:
                conn.execute(
                    "INSERT INTO page_doc_type_history "
                    "(page_id, company_id, file_name, page_no, old_doc_type_name, "
                    " new_doc_type_name, reason, workflow_id, decision_id, confirmed_by) "
                    "SELECT page_id, company_id, file_name, page_no, ?, ?, ?, ?, ?, 'web_viewer' "
                    "FROM pages WHERE page_id = ?",
                    (
                        old_doc,
                        body.doc_type_name,
                        "web_viewer manual reclassification",
                        f"web_viewer_{datetime.now().strftime('%Y%m%d')}",
                        str(uuid.uuid4()),
                        page_id,
                    ),
                )

        if body.doc_type_secondary is not None:
            sec_val = body.doc_type_secondary if body.doc_type_secondary else None
            updates.append("doc_type_secondary = ?")
            params.append(sec_val)
            conn.execute(
                "INSERT INTO field_reviews (company_id, field_name, confirmed_value, confirmed_by) "
                "VALUES (?, 'doc_type_secondary', ?, 'web_viewer')",
                (company_id, sec_val or ""),
            )

        if body.rotation is not None:
            updates.append("rotation = ?")
            params.append(body.rotation)
            conn.execute(
                "INSERT INTO field_reviews (company_id, field_name, confirmed_value, confirmed_by) "
                "VALUES (?, 'rotation', ?, 'web_viewer')",
                (company_id, str(body.rotation)),
            )

        if not updates:
            raise HTTPException(400, "No fields to update")

        params.append(page_id)
        conn.execute(
            f"UPDATE pages SET {', '.join(updates)} WHERE page_id = ?",
            params,
        )
        conn.commit()

        updated = conn.execute(
            "SELECT page_id, file_name, page_no, doc_type_name, doc_type_secondary, "
            "doc_type_id, rotation, confidence FROM pages WHERE page_id = ?",
            (page_id,),
        ).fetchone()
        return dict(updated)
    finally:
        conn.close()
