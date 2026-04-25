from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.database import get_db

router = APIRouter(prefix="/api/permits", tags=["permits"])


class PermitUpdate(BaseModel):
    permit_number: str | None = None
    permit_authority: str | None = None
    permit_category: str | None = None
    permit_year: str | None = None
    issue_date: str | None = None
    expiry_date: str | None = None
    trade_names: str | None = None  # カンマ区切り


@router.patch("/{permit_id}")
def update_permit(permit_id: int, body: PermitUpdate):
    """permit のフィールドを更新。trade_names は permit_trades を入れ替え。"""
    conn = get_db()
    try:
        existing = conn.execute(
            "SELECT permit_id FROM permits WHERE permit_id = ?", (permit_id,)
        ).fetchone()
        if not existing:
            raise HTTPException(404, f"Permit not found: {permit_id}")

        fields = {}
        for k in ["permit_number", "permit_authority", "permit_category",
                  "permit_year", "issue_date", "expiry_date"]:
            v = getattr(body, k)
            if v is not None:
                fields[k] = v if v != "" else None

        if fields:
            set_clause = ", ".join(f"{k}=?" for k in fields.keys())
            conn.execute(
                f"UPDATE permits SET {set_clause}, updated_at=datetime('now','localtime') WHERE permit_id=?",
                (*fields.values(), permit_id),
            )

        # trade_names を更新（カンマ or 、区切り）
        if body.trade_names is not None:
            conn.execute("DELETE FROM permit_trades WHERE permit_id=?", (permit_id,))
            trades = [t.strip() for t in body.trade_names.replace("、", ",").split(",") if t.strip()]
            for t in trades:
                conn.execute(
                    "INSERT INTO permit_trades (permit_id, trade_name) VALUES (?, ?)",
                    (permit_id, t),
                )

        conn.commit()

        # 更新後を返す
        row = conn.execute(
            "SELECT p.permit_id, p.permit_number, p.permit_authority, "
            "p.permit_category, p.permit_year, p.issue_date, p.expiry_date, "
            "GROUP_CONCAT(pt.trade_name, '、') AS trade_names "
            "FROM permits p LEFT JOIN permit_trades pt ON pt.permit_id = p.permit_id "
            "WHERE p.permit_id = ? GROUP BY p.permit_id",
            (permit_id,),
        ).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


class PermitCreate(BaseModel):
    company_id: str
    permit_number: str | None = ""
    permit_authority: str | None = ""
    permit_category: str | None = ""
    permit_year: str | None = ""
    issue_date: str | None = None
    expiry_date: str | None = None
    trade_names: str | None = ""


@router.post("")
def create_permit(body: PermitCreate):
    """新規許可証レコードを作成（手動登録用）"""
    conn = get_db()
    try:
        comp = conn.execute(
            "SELECT company_id FROM companies WHERE company_id = ?", (body.company_id,)
        ).fetchone()
        if not comp:
            raise HTTPException(404, f"Company not found: {body.company_id}")

        cur = conn.execute(
            "INSERT INTO permits (company_id, permit_number, permit_authority, permit_category, "
            "permit_year, issue_date, expiry_date, current_flag, source) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 1, 'manual')",
            (body.company_id, body.permit_number or None, body.permit_authority or None,
             body.permit_category or None, body.permit_year or None,
             body.issue_date or None, body.expiry_date or None),
        )
        permit_id = cur.lastrowid

        if body.trade_names:
            trades = [t.strip() for t in body.trade_names.replace("、", ",").split(",") if t.strip()]
            for t in trades:
                conn.execute(
                    "INSERT INTO permit_trades (permit_id, trade_name) VALUES (?, ?)",
                    (permit_id, t),
                )
        conn.commit()

        row = conn.execute(
            "SELECT p.permit_id, p.permit_number, p.permit_authority, "
            "p.permit_category, p.permit_year, p.issue_date, p.expiry_date, "
            "GROUP_CONCAT(pt.trade_name, '、') AS trade_names "
            "FROM permits p LEFT JOIN permit_trades pt ON pt.permit_id = p.permit_id "
            "WHERE p.permit_id = ? GROUP BY p.permit_id",
            (permit_id,),
        ).fetchone()
        return dict(row) if row else {"permit_id": permit_id}
    finally:
        conn.close()


@router.get("/{permit_id}/mlit_url")
def mlit_search_url(permit_id: int):
    """MLIT 検索URLを生成（手動で開く用）"""
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT p.permit_number, p.permit_authority, c.official_name "
            "FROM permits p JOIN companies c ON c.company_id = p.company_id "
            "WHERE p.permit_id = ?",
            (permit_id,),
        ).fetchone()
        if not row:
            raise HTTPException(404, f"Permit not found: {permit_id}")

        base = "https://etsuran2.mlit.go.jp/TAKKEN/kensetsuKensaku.do"
        return {
            "url": base,
            "company_name": row["official_name"],
            "permit_number": row["permit_number"],
            "permit_authority": row["permit_authority"],
        }
    finally:
        conn.close()
