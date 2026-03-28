import re
from io import BytesIO

import fitz  # PyMuPDF
from PIL import Image
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from app.database import ORIGINALS_DIR, get_db

router = APIRouter(prefix="/api/images", tags=["images"])


def _find_company_folder(company_id: str):
    """Find the originals folder matching company_id (e.g. C0008_会社名/)."""
    pattern = re.compile(rf"^{re.escape(company_id)}_")
    for folder in ORIGINALS_DIR.iterdir():
        if folder.is_dir() and pattern.match(folder.name):
            return folder
    return None


@router.get("/{company_id}/{filename}/{page_no}")
def get_page_image(
    company_id: str,
    filename: str,
    page_no: int,
    dpi: int = Query(150, ge=50, le=600),
):
    folder = _find_company_folder(company_id)
    if folder is None:
        raise HTTPException(404, f"Company folder not found: {company_id}")

    pdf_path = (folder / filename).resolve()
    if pdf_path.parent != folder.resolve():
        raise HTTPException(400, "Invalid filename")
    if not pdf_path.is_file():
        raise HTTPException(404, f"PDF not found: {filename}")

    try:
        doc = fitz.open(str(pdf_path))
    except Exception as exc:
        raise HTTPException(500, f"Cannot open PDF: {exc}")

    if page_no < 1 or page_no > len(doc):
        doc.close()
        raise HTTPException(404, f"Page {page_no} out of range (1-{len(doc)})")

    page = doc[page_no - 1]
    mat = fitz.Matrix(dpi / 72, dpi / 72)
    pix = page.get_pixmap(matrix=mat)
    img_bytes = pix.tobytes("jpeg", jpg_quality=85)
    doc.close()

    # DBから回転値を取得して適用
    conn = get_db()
    rot_row = conn.execute(
        "SELECT rotation FROM pages WHERE company_id=? AND file_name=? AND page_no=?",
        (company_id, filename, page_no),
    ).fetchone()
    conn.close()
    rotation = rot_row[0] if rot_row else 0

    if rotation != 0:
        img = Image.open(BytesIO(img_bytes))
        rot_map = {90: -90, 180: 180, 270: 90}
        img = img.rotate(rot_map.get(rotation, 0), expand=True)
        buf = BytesIO()
        img.save(buf, format="JPEG", quality=85)
        img_bytes = buf.getvalue()

    return StreamingResponse(
        BytesIO(img_bytes),
        media_type="image/jpeg",
        headers={"Cache-Control": "no-cache"},
    )
