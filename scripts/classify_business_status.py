"""
Phase R-D: 業務判定算出エンジン (機械化)。

業務判定軸 (有効/書類不備/未提出/期限90日以内/期限切れ/対象外) を機械的に算出。

入力: 当方 v2/v3 一覧表 + DB (companies, permits, company_doc_exemptions, MLITPermits 同期)
出力: data/business_status_<TS>.csv

判定ロジック:
  1. status == 'INACTIVE' / 'MERGED' / 'MANAGED_OUT' → 対象外
  2. mlit_expiry < today                            → 期限切れ
  3. mlit_expiry < today + 90日                     → 期限90日以内
  4. 受領済み == 0                                  → 未提出
  5. 受領済み >= (8 - exemption)                    → 有効
  6. else                                            → 書類不備

※ MLIT 期限が取得できない場合は添付許可証期限 (一覧表の 有効期限列) を使用

Usage:
  python scripts/classify_business_status.py [--ts <TS>]
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path

import openpyxl

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT / "data" / "permit_tracker.db"


def _latest_review_xlsx() -> Path:
    """FDE_MANAGED の最新 00_一覧表_v3_*.xlsx を返す。なければ v2。"""
    fde = PROJECT / "output" / "FDE_MANAGED"
    v3 = sorted(fde.glob("00_一覧表_v3_*.xlsx"), reverse=True)
    if v3:
        return v3[0]
    v2 = sorted(fde.glob("00_一覧表_v2_*.xlsx"), reverse=True)
    if v2:
        return v2[0]
    raise FileNotFoundError("一覧表 xlsx が見つかりません")


V2_XLSX = _latest_review_xlsx()

DOC_COLS = ["取引申請書", "建設業許可証", "決算書", "工事経歴書",
             "取引先一覧表", "労働安全衛生誓約書", "資格略字一覧", "労働者名簿"]


def parse_date(v) -> date | None:
    if not v:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = str(v).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


def load_v2_rows() -> list[dict]:
    wb = openpyxl.load_workbook(V2_XLSX, data_only=True)
    ws = wb["マスタ145社受領状況"]
    rows = []
    for r in range(2, ws.max_row + 1):
        cid = ws.cell(r, 2).value
        if not cid:
            continue
        row = {
            "company_id": cid,
            "master_name": ws.cell(r, 3).value,
            "db_name": ws.cell(r, 4).value,
            "state": ws.cell(r, 6).value,
            "expiry": parse_date(ws.cell(r, 12).value),
            "soroi": ws.cell(r, 22).value,
        }
        for i, dc in enumerate(DOC_COLS):
            row[dc] = ws.cell(r, 14 + i).value
        rows.append(row)
    return rows


def load_db_companies(conn: sqlite3.Connection) -> dict[str, dict]:
    out = {}
    for r in conn.execute(
        "SELECT company_id, official_name, status FROM companies"
    ):
        out[r[0]] = {"company_id": r[0], "official_name": r[1], "status": r[2]}
    return out


def load_exemptions(conn: sqlite3.Connection) -> dict[str, set[str]]:
    """company_id -> set of exempted document_type"""
    out = {}
    for r in conn.execute(
        "SELECT company_id, document_type FROM company_doc_exemptions "
        "WHERE exempt_kind IN ('NOT_APPLICABLE', 'ALTERNATIVE')"
    ):
        out.setdefault(r[0], set()).add(r[1])
    return out


def classify(row: dict, db_status: str | None, exempts: set[str], today: date) -> dict:
    """業務判定算出"""
    eff_status = db_status or row.get("state")
    if eff_status in ("INACTIVE", "MERGED", "MANAGED_OUT"):
        return {"status": "対象外", "reason": f"DB status={eff_status}"}

    expiry = row.get("expiry")
    if expiry:
        days_until = (expiry - today).days
        if days_until < 0:
            return {"status": "期限切れ", "reason": f"{expiry} ({-days_until}日経過)"}
        if days_until < 90:
            return {"status": "期限90日以内", "reason": f"{expiry} (残{days_until}日)"}

    # 受領状況判定: ○ または — (対象外) もカウント
    received = sum(1 for dc in DOC_COLS if row.get(dc) in ("○", "〇", "—"))
    required = 8

    if received == 0:
        return {"status": "未提出", "reason": "全 8 書類受領なし"}
    if received >= required:
        return {"status": "有効", "reason": f"{received}/8 全揃い"}
    return {"status": "書類不備", "reason": f"{received}/8 (不足 {required-received})"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ts", default=datetime.now().strftime("%Y%m%d_%H%M%S"))
    ap.add_argument("--today", help="判定基準日 (YYYY-MM-DD)。省略時は今日")
    args = ap.parse_args()

    today = parse_date(args.today) if args.today else date.today()

    print(f"=== Phase R-D: 業務判定算出 ===")
    print(f"  TS: {args.ts}")
    print(f"  判定基準日: {today}")
    print(f"  使用 xlsx: {V2_XLSX.name}")

    conn = sqlite3.connect(DB_PATH)
    v2_rows = load_v2_rows()
    db_companies = load_db_companies(conn)
    exemptions = load_exemptions(conn)
    conn.close()

    print(f"  一覧表: {len(v2_rows)} 社")
    print(f"  DB companies: {len(db_companies)} 社")
    print(f"  exemptions: {sum(len(v) for v in exemptions.values())} 項目 / {len(exemptions)} 社")

    results = []
    from collections import Counter
    status_count = Counter()

    for row in v2_rows:
        cid = row["company_id"]
        db_status = db_companies.get(cid, {}).get("status")
        exempts = exemptions.get(cid, set())
        verdict = classify(row, db_status, exempts, today)
        received = sum(1 for dc in DOC_COLS if row.get(dc) in ("○", "〇", "—"))
        results.append({
            "company_id": cid,
            "master_name": row.get("master_name") or row.get("db_name"),
            "業務判定": verdict["status"],
            "判定理由": verdict["reason"],
            "受領済み": received,
            "実質必要": 8 - len(exempts),
            "exempts": "|".join(sorted(exempts)) if exempts else "",
            "DB_status": db_status,
            "expiry": str(row.get("expiry") or ""),
        })
        status_count[verdict["status"]] += 1

    print()
    print("業務判定 内訳:")
    for k, v in status_count.most_common():
        print(f"  {k}: {v}")

    out = PROJECT / "data" / f"business_status_{args.ts}.csv"
    with out.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["company_id", "master_name", "業務判定", "判定理由",
                    "受領済み", "実質必要", "exempts", "DB_status", "expiry"])
        for r in results:
            w.writerow([r["company_id"], r["master_name"], r["業務判定"],
                        r["判定理由"], r["受領済み"], r["実質必要"],
                        r["exempts"], r["DB_status"], r["expiry"]])

    print(f"\n出力: {out.relative_to(PROJECT)}")

    print()
    print("原本集計 (参考):")
    print("  有効 85 / 書類不備 12 / 未提出 30 / 期限90日以内 1 / 期限切れ 1 / 対象外 16")


if __name__ == "__main__":
    main()
