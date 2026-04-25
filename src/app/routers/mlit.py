"""
MLIT 確認エンドポイント。
- POST /api/companies/{cid}/mlit_refresh: 個別会社の MLIT 最新化（同期、約15秒待ち）
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
import re

from fastapi import APIRouter, HTTPException

from app.database import get_db

router = APIRouter(prefix="/api", tags=["mlit"])

PROJECT = Path(__file__).resolve().parent.parent.parent.parent
SCRIPT = PROJECT / "scripts" / "mlit_refresh_one.py"


@router.post("/companies/{company_id}/mlit_refresh")
def mlit_refresh_one(company_id: str):
    """個別会社の MLIT 確認を実行（subprocess、同期）"""
    if not re.match(r"^C\d{4}$", company_id):
        raise HTTPException(400, "invalid company_id format")

    # kill switch チェック
    pause_file = Path(r"c:/tmp/mlit_pause")
    if pause_file.exists():
        raise HTTPException(503, f"MLIT 確認は一時停止中です（{pause_file} を削除すると再開）")

    # 会社存在チェック
    conn = get_db()
    try:
        comp = conn.execute(
            "SELECT company_id, official_name FROM companies WHERE company_id=?",
            (company_id,),
        ).fetchone()
        if not comp:
            raise HTTPException(404, f"Company not found: {company_id}")
    finally:
        conn.close()

    cmd = [sys.executable, str(SCRIPT), "--company-id", company_id, "--json"]
    try:
        proc = subprocess.run(
            cmd, cwd=str(PROJECT), capture_output=True, text=True, encoding="utf-8",
            timeout=60,  # 規約3秒+ページ遷移+余裕で60秒上限
        )
    except subprocess.TimeoutExpired:
        raise HTTPException(504, "MLIT 確認がタイムアウトしました（60秒）")

    if proc.returncode != 0:
        raise HTTPException(500, f"MLIT 確認失敗: {proc.stderr.strip() or proc.stdout.strip()}")

    try:
        result = json.loads(proc.stdout.strip().split("\n")[-1])
    except Exception:
        raise HTTPException(500, f"結果パース失敗: {proc.stdout!r}")

    return result
