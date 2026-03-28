"""
DB差分問題修正スクリプト

修正1: 許可番号OCR桁違い（19社）
修正2: 有効期限の発行日/満了日混同（23社）
修正3: 社名表記揺れ

使用法:
    python src/fix_db_issues.py
"""
import csv
import json
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

sys.stdout.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# パス定義
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent
SRC_DIR = Path(__file__).parent
sys.path.insert(0, str(SRC_DIR))

from db import DB_PATH, get_connection

COMPANY_MASTER = PROJECT_ROOT / "output" / "company_master.csv"
EXCEL_145 = Path("C:/tmp/継続取引業者リスト_145社.xlsx")
REOCR_RESULTS = Path("C:/tmp/permit_reocr_results.json")
OCR_RESULTS = Path("C:/tmp/permit_ocr_results.csv")

# 手動確認済み7社のcompany_id（上書きしない）
MANUAL_REVIEWED_CIDS = {
    "C0022",  # 有限会社キー・カンパニー
    "C0060",  # 株式会社井上商会
    "C0064",  # 株式会社オカケン
    "C0054",  # 株式会社鈴木産業
    "C0034",  # 株式会社メンテック
    "C0056",  # 福釜電工株式会社
    "C0025",  # 有限会社 竹下建築
}


def _read_csv_sig(path: Path) -> list[dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def _load_reocr() -> dict[str, dict]:
    """reocr結果を社名→データのmapで返す"""
    if not REOCR_RESULTS.exists():
        return {}
    with open(REOCR_RESULTS, "r", encoding="utf-8") as f:
        data = json.load(f)
    result: dict[str, dict] = {}
    for item in data:
        company = item.get("company", "").strip()
        if company:
            result[company] = item
    return result


def _load_ocr_csv() -> dict[str, list[dict]]:
    """OCR CSV結果を社名→データリストのmapで返す"""
    if not OCR_RESULTS.exists():
        return {}
    rows = _read_csv_sig(OCR_RESULTS)
    result: dict[str, list[dict]] = {}
    for r in rows:
        company = r.get("company", "").strip()
        if company:
            result.setdefault(company, []).append(r)
    return result


def _load_excel_permits() -> dict[str, dict]:
    """Excelから許可データを読み込む（社名→データのmap）"""
    import openpyxl
    wb = openpyxl.load_workbook(str(EXCEL_145), read_only=True)
    ws = wb.active
    result: dict[str, dict] = {}
    seen: set[str] = set()
    for row in ws.iter_rows(min_row=2, values_only=True):
        name = str(row[0] or "").strip()
        if not name or "@" in name or name in seen:
            continue
        seen.add(name)

        pn_f = str(row[5] or "").strip()
        pn_h = str(row[7] or "").strip() if len(row) > 7 else ""
        pn_y = str(row[24] or "").strip() if len(row) > 24 else ""
        auth = str(row[25] or "").strip() if len(row) > 25 else ""
        cat = str(row[26] or "").strip() if len(row) > 26 else ""
        exp = str(row[27] or "").strip() if len(row) > 27 else ""

        # 許可番号の決定ロジック:
        # H列はOCR取込値で信頼度が低い場合がある（短い数値はページ数等の誤り）
        # Y列 = Excel手入力の許可番号（最も信頼度が高い）
        # F列 = 元の許可番号欄（般/特が入ることもある）
        # 優先度: Y列(数字5桁以上) > H列(数字5桁以上) > F列(数字5桁以上) > H列(数字4桁) > Y列(数字4桁)
        permit_number = ""
        # まずY列を最優先（手入力）
        y_clean = pn_y.lstrip("0")
        h_clean = pn_h.lstrip("0")
        f_clean = pn_f.lstrip("0")
        # 5桁以上の数字を優先
        for candidate in [y_clean, h_clean, f_clean]:
            if candidate and candidate.isdigit() and len(candidate) >= 4:
                permit_number = candidate
                break
        # なければ短い数字も許容
        if not permit_number:
            for candidate in [y_clean, h_clean, f_clean]:
                if candidate and candidate.isdigit():
                    permit_number = candidate
                    break

        result[name] = {
            "permit_number": permit_number,
            "permit_number_h": pn_h,
            "permit_number_y": pn_y,
            "permit_number_f": pn_f,
            "permit_authority": auth,
            "permit_category": cat,
            "expiry_date": exp,
        }
    wb.close()
    return result


def _load_company_master() -> dict[str, dict[str, str]]:
    """company_master.csv を company_id → データ のmapで返す"""
    if not COMPANY_MASTER.exists():
        return {}
    rows = _read_csv_sig(COMPANY_MASTER)
    result: dict[str, dict[str, str]] = {}
    for r in rows:
        cid = r.get("company_id", "")
        if cid:
            result[cid] = r
    return result


def _resolve_company_name(conn: sqlite3.Connection, name: str) -> Optional[str]:
    """社名からcompany_idを取得（完全一致→部分一致）"""
    row = conn.execute(
        "SELECT company_id FROM companies WHERE official_name = ?", (name,)
    ).fetchone()
    if row:
        return row["company_id"]
    # エイリアス検索
    row = conn.execute(
        "SELECT company_id FROM companies WHERE name_aliases LIKE ?",
        (f"%{name}%",)
    ).fetchone()
    if row:
        return row["company_id"]
    # 部分一致
    for r in conn.execute("SELECT company_id, official_name FROM companies").fetchall():
        if name in r["official_name"] or r["official_name"] in name:
            return r["company_id"]
    return None


def _record_review(
    conn: sqlite3.Connection,
    company_id: str,
    field_name: str,
    value: str,
    confirmed_by: str = "auto_fix",
) -> None:
    """field_reviewsに修正記録を追加"""
    conn.execute(
        """INSERT INTO field_reviews
           (company_id, field_name, confirmed_value, confirmed_by)
           VALUES (?,?,?,?)""",
        (company_id, field_name, value, confirmed_by),
    )


def _add_5years_minus_1day(date_str: str) -> Optional[str]:
    """日付文字列にて issue_date + 5年 - 1日 を計算"""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        # 5年後 - 1日
        expiry = dt.replace(year=dt.year + 5) - timedelta(days=1)
        return expiry.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return None


# ===========================================================================
# 修正1: 許可番号OCR桁違い
# ===========================================================================
def fix_permit_numbers(conn: sqlite3.Connection) -> int:
    """許可番号の桁違いを修正"""
    print("\n" + "=" * 70)
    print(" 修正1: 許可番号OCR桁違い")
    print("=" * 70)

    excel_data = _load_excel_permits()
    reocr_data = _load_reocr()
    ocr_csv_data = _load_ocr_csv()
    fixed_count = 0

    # 現在有効な許可証を取得
    permits = conn.execute(
        """SELECT p.permit_id, p.company_id, c.official_name, p.permit_number,
                  p.source
           FROM permits p
           JOIN companies c ON c.company_id = p.company_id
           WHERE p.current_flag = 1"""
    ).fetchall()

    for permit in permits:
        pid = permit["permit_id"]
        cid = permit["company_id"]
        name = permit["official_name"]
        db_pn = (permit["permit_number"] or "").lstrip("0")
        source = permit["source"]

        # 手動確認済み7社はスキップ
        if cid in MANUAL_REVIEWED_CIDS:
            continue

        # Excelの値を取得（社名完全一致 → 部分一致）
        excel_pn = ""
        excel_entry = excel_data.get(name)
        if not excel_entry:
            for ename, edata in excel_data.items():
                if name in ename or ename in name:
                    excel_entry = edata
                    break
        if excel_entry:
            excel_pn = excel_entry["permit_number"]

        if not excel_pn or not db_pn:
            continue

        # 一致ならスキップ
        if db_pn == excel_pn:
            continue

        # 不一致 → 正しい値を決定
        # 優先度:
        #   1. reocr確定値（permit_reocr_results.json）→ 最優先
        #   2. ExcelのH列とreocr値が一致 → reocr値を正とする
        #   3. Excel Y列（手入力）→ reocrがない場合のフォールバック
        correct_pn = None
        correct_source = ""

        # reocr結果を確認（社名部分一致で検索）
        reocr_entry = None
        for rname, rdata in reocr_data.items():
            if rname in name or name in rname:
                reocr_entry = rdata
                break
        if reocr_entry:
            reocr_pn = str(reocr_entry.get("permit_number", "")).strip().lstrip("0")
            if reocr_pn and reocr_pn != "UNCERTAIN":
                # reocr値がDB値と一致 → 変更不要（reocr確定値=DB値）
                if reocr_pn == db_pn:
                    continue
                # reocr値がExcelと一致 → reocr+excel合意
                if reocr_pn == excel_pn:
                    correct_pn = reocr_pn
                    correct_source = "reocr+excel"
                else:
                    # reocr値がDB/Excelどちらとも違う → reocr値を正とする
                    correct_pn = reocr_pn
                    correct_source = "reocr"

        # reocrがない場合 → Excel値を使う
        if correct_pn is None:
            correct_pn = excel_pn
            correct_source = "excel"

            # OCR CSV結果でExcel値と合致するものがあるか確認
            for oname, orows in ocr_csv_data.items():
                if oname in name or name in oname:
                    for orow in orows:
                        ocr_pn = orow.get("permit_number", "").strip().lstrip("0")
                        if ocr_pn == excel_pn:
                            correct_source = "ocr_csv+excel"
                            break
                    break

        print(f"  [{name}] DB={db_pn} -> {correct_pn} ({correct_source})")

        # permits更新
        conn.execute(
            "UPDATE permits SET permit_number=?, updated_at=datetime('now','localtime') WHERE permit_id=?",
            (correct_pn, pid),
        )
        # field_reviewsに記録
        _record_review(conn, cid, "permit_number", correct_pn, "auto_fix")
        fixed_count += 1

    conn.commit()
    print(f"\n  -> 許可番号修正: {fixed_count} 件")
    return fixed_count


# ===========================================================================
# 修正2: 有効期限の発行日/満了日混同
# ===========================================================================
def fix_expiry_dates(conn: sqlite3.Connection) -> int:
    """有効期限の発行日/満了日混同を修正"""
    print("\n" + "=" * 70)
    print(" 修正2: 有効期限の発行日/満了日混同")
    print("=" * 70)

    reocr_data = _load_reocr()
    fixed_count = 0

    permits = conn.execute(
        """SELECT p.permit_id, p.company_id, c.official_name,
                  p.issue_date, p.expiry_date, p.source
           FROM permits p
           JOIN companies c ON c.company_id = p.company_id
           WHERE p.current_flag = 1"""
    ).fetchall()

    for permit in permits:
        pid = permit["permit_id"]
        cid = permit["company_id"]
        name = permit["official_name"]
        issue = permit["issue_date"] or ""
        expiry = permit["expiry_date"] or ""
        source = permit["source"]

        # 手動確認済み7社はスキップ
        if cid in MANUAL_REVIEWED_CIDS:
            continue

        # reocr結果を確認（最優先）
        reocr_entry = None
        for rname, rdata in reocr_data.items():
            if rname in name or name in rname:
                reocr_entry = rdata
                break

        if reocr_entry:
            reocr_issue = str(reocr_entry.get("issue_date", "")).strip()
            reocr_expiry = str(reocr_entry.get("expiry_date", "")).strip()
            if reocr_issue and reocr_issue != "UNCERTAIN" and reocr_expiry and reocr_expiry != "UNCERTAIN":
                # reocr値がissue_date + 5年 - 1日のルールに合致するか確認
                expected_expiry = _add_5years_minus_1day(reocr_issue)
                if expected_expiry and expected_expiry == reocr_expiry:
                    # reocr値は正しい → DB値と比較
                    if issue != reocr_issue or expiry != reocr_expiry:
                        print(f"  [{name}] reocr適用: issue={issue}->{reocr_issue} expiry={expiry}->{reocr_expiry}")
                        conn.execute(
                            """UPDATE permits
                               SET issue_date=?, expiry_date=?,
                                   updated_at=datetime('now','localtime')
                               WHERE permit_id=?""",
                            (reocr_issue, reocr_expiry, pid),
                        )
                        _record_review(conn, cid, "issue_date", reocr_issue, "auto_fix_reocr")
                        _record_review(conn, cid, "expiry_date", reocr_expiry, "auto_fix_reocr")
                        fixed_count += 1
                    continue  # reocrがある場合は以降のルールベース修正をスキップ

        # issue_date/expiry_dateの日付チェック
        if not issue or not expiry or expiry == "UNCERTAIN" or issue == "UNCERTAIN":
            # 日付が不完全 → issue_dateだけあれば計算可能
            if issue and issue != "UNCERTAIN" and (not expiry or expiry == "UNCERTAIN"):
                calc_expiry = _add_5years_minus_1day(issue)
                if calc_expiry:
                    print(f"  [{name}] expiry計算: issue={issue} -> expiry={calc_expiry}")
                    conn.execute(
                        """UPDATE permits SET expiry_date=?,
                               updated_at=datetime('now','localtime')
                           WHERE permit_id=?""",
                        (calc_expiry, pid),
                    )
                    _record_review(conn, cid, "expiry_date", calc_expiry, "auto_fix_calc")
                    fixed_count += 1
            continue

        # 両方の日付がある場合、5年ルールで検証
        try:
            issue_dt = datetime.strptime(issue, "%Y-%m-%d")
            expiry_dt = datetime.strptime(expiry, "%Y-%m-%d")
        except ValueError:
            continue

        days_diff = (expiry_dt - issue_dt).days
        expected_expiry = _add_5years_minus_1day(issue)

        # 正常: 約1826日（5年 - 1日）付近（±30日の許容範囲）
        # 建設業許可の有効期限は「許可日+5年-1日」だが、OCR/reocr値が数日ずれるのは
        # 許可日の認識ずれが原因。大幅なズレでなければDB値を信頼して変更しない。
        if 1790 <= days_diff <= 1860:
            continue

        # 異常: expiry_dateがissue_dateより前 → 明らかに逆
        if expiry_dt < issue_dt:
            print(f"  [{name}] 逆転修正: issue={issue} expiry={expiry} -> swap+計算")
            # 小さい方がissue_date、5年ルールで再計算
            real_issue = expiry  # 逆なので
            real_expiry = _add_5years_minus_1day(real_issue)
            if real_expiry:
                conn.execute(
                    """UPDATE permits SET issue_date=?, expiry_date=?,
                           updated_at=datetime('now','localtime')
                       WHERE permit_id=?""",
                    (real_issue, real_expiry, pid),
                )
                _record_review(conn, cid, "issue_date", real_issue, "auto_fix_swap")
                _record_review(conn, cid, "expiry_date", real_expiry, "auto_fix_swap")
                fixed_count += 1
            continue

        # 異常: 5年差ではない（差が小さすぎる/大きすぎる）
        if days_diff < 1790:
            # 差が小さい → expiry_dateが実はissue_dateのコピーまたは近い値
            # issue_dateを信頼して再計算
            if expected_expiry:
                print(f"  [{name}] 期間不足修正: issue={issue} expiry={expiry}->{expected_expiry} (diff={days_diff}日)")
                conn.execute(
                    """UPDATE permits SET expiry_date=?,
                           updated_at=datetime('now','localtime')
                       WHERE permit_id=?""",
                    (expected_expiry, pid),
                )
                _record_review(conn, cid, "expiry_date", expected_expiry, "auto_fix_period")
                fixed_count += 1
        elif days_diff > 1860:
            # 差が大きすぎる → 発行日と満了日がどちらか間違っている
            # expiry_dateから逆算したissue_date候補を計算
            # 注: replace(year-5)で正確に逆算（閏年対応）
            try:
                calc_issue_dt = expiry_dt.replace(year=expiry_dt.year - 5) + timedelta(days=1)
            except ValueError:
                # 2/29等の問題
                calc_issue_dt = expiry_dt.replace(year=expiry_dt.year - 5, day=28) + timedelta(days=1)
            calc_issue = calc_issue_dt.strftime("%Y-%m-%d")
            # 逆算issue_dateから再計算したexpiryがexpiry_dateと一致するか確認
            verify_expiry = _add_5years_minus_1day(calc_issue)
            if verify_expiry:
                verify_diff = abs((datetime.strptime(verify_expiry, "%Y-%m-%d") - expiry_dt).days)
                if verify_diff <= 2:
                    # expiry_dateは正しく、issue_dateが間違い → issue修正
                    print(f"  [{name}] 期間過大→issue修正: issue={issue}->{calc_issue} expiry={expiry} (diff={days_diff}日)")
                    conn.execute(
                        """UPDATE permits SET issue_date=?,
                               updated_at=datetime('now','localtime')
                           WHERE permit_id=?""",
                        (calc_issue, pid),
                    )
                    _record_review(conn, cid, "issue_date", calc_issue, "auto_fix_period")
                    fixed_count += 1
                else:
                    # issue_dateを信頼してexpiry再計算
                    if expected_expiry:
                        print(f"  [{name}] 期間過大→expiry再計算: issue={issue} expiry={expiry}->{expected_expiry} (diff={days_diff}日)")
                        conn.execute(
                            """UPDATE permits SET expiry_date=?,
                                   updated_at=datetime('now','localtime')
                               WHERE permit_id=?""",
                            (expected_expiry, pid),
                        )
                        _record_review(conn, cid, "expiry_date", expected_expiry, "auto_fix_period")
                        fixed_count += 1

    conn.commit()
    print(f"\n  -> 有効期限修正: {fixed_count} 件")
    return fixed_count


# ===========================================================================
# 修正3: 社名表記揺れ
# ===========================================================================
def fix_company_names(conn: sqlite3.Connection) -> int:
    """社名表記揺れを修正"""
    print("\n" + "=" * 70)
    print(" 修正3: 社名表記揺れ")
    print("=" * 70)

    master = _load_company_master()
    fixed_count = 0

    # company_master.csvのofficial_nameを正として、companiesテーブルを更新
    for cid, mdata in master.items():
        master_name = mdata.get("official_name", "").strip()
        if not master_name:
            continue

        row = conn.execute(
            "SELECT official_name, name_aliases FROM companies WHERE company_id=?",
            (cid,)
        ).fetchone()
        if not row:
            continue

        db_name = row["official_name"]
        db_aliases = row["name_aliases"] or ""

        if db_name != master_name:
            print(f"  [{cid}] 社名修正: {db_name} -> {master_name}")
            conn.execute(
                """UPDATE companies SET official_name=?,
                       updated_at=datetime('now','localtime')
                   WHERE company_id=?""",
                (master_name, cid),
            )
            fixed_count += 1

        # エイリアス更新（master側の情報で補完）
        master_aliases = mdata.get("name_aliases", "").strip()
        if master_aliases and master_aliases != db_aliases:
            # マスタのエイリアスが既存と異なる場合、マスタ値で更新
            print(f"  [{cid}] エイリアス更新: {db_aliases} -> {master_aliases}")
            conn.execute(
                """UPDATE companies SET name_aliases=?,
                       updated_at=datetime('now','localtime')
                   WHERE company_id=?""",
                (master_aliases, cid),
            )
            fixed_count += 1

    # 特定の修正: メンテック関連
    # Excelでは「株式会社Man-tech」として出るが、正式名は「株式会社メンテック」
    row = conn.execute(
        "SELECT company_id, official_name, name_aliases FROM companies WHERE company_id='C0034'"
    ).fetchone()
    if row:
        current_aliases = row["name_aliases"] or ""
        if "Man-tech" not in current_aliases:
            new_aliases = current_aliases + "|Man-tech" if current_aliases else "Man-tech"
            print(f"  [C0034] Man-techエイリアス追加: {current_aliases} -> {new_aliases}")
            conn.execute(
                """UPDATE companies SET name_aliases=?,
                       updated_at=datetime('now','localtime')
                   WHERE company_id='C0034'""",
                (new_aliases,),
            )
            fixed_count += 1

    conn.commit()
    print(f"\n  -> 社名修正: {fixed_count} 件")
    return fixed_count


# ===========================================================================
# 検証
# ===========================================================================
def verify(conn: sqlite3.Connection) -> None:
    """修正後の検証"""
    print("\n" + "=" * 70)
    print(" 検証: v_current_permits")
    print("=" * 70)

    total = conn.execute("SELECT count(*) FROM permits").fetchone()[0]
    current = conn.execute("SELECT count(*) FROM permits WHERE current_flag=1").fetchone()[0]
    reviews = conn.execute("SELECT count(*) FROM field_reviews").fetchone()[0]
    auto_fixes = conn.execute(
        "SELECT count(*) FROM field_reviews WHERE confirmed_by LIKE 'auto_fix%'"
    ).fetchone()[0]

    print(f"  permits total: {total}")
    print(f"  permits current: {current}")
    print(f"  field_reviews total: {reviews}")
    print(f"  field_reviews auto_fix: {auto_fixes}")

    # v_current_permits件数
    vcp = conn.execute("SELECT count(*) FROM v_current_permits").fetchone()[0]
    print(f"  v_current_permits: {vcp}")

    # 残存問題チェック
    print("\n--- 残存: expiry=UNCERTAIN ---")
    for row in conn.execute(
        """SELECT c.official_name, p.permit_number, p.expiry_date
           FROM permits p JOIN companies c ON c.company_id=p.company_id
           WHERE p.current_flag=1 AND (p.expiry_date IS NULL OR p.expiry_date='UNCERTAIN')"""
    ).fetchall():
        print(f"  {row['official_name']}: pn={row['permit_number']} expiry={row['expiry_date']}")

    print("\n--- 残存: 5年ルール不整合 ---")
    for row in conn.execute(
        """SELECT c.official_name, p.issue_date, p.expiry_date
           FROM permits p JOIN companies c ON c.company_id=p.company_id
           WHERE p.current_flag=1
             AND p.issue_date IS NOT NULL AND p.issue_date != ''
             AND p.expiry_date IS NOT NULL AND p.expiry_date != '' AND p.expiry_date != 'UNCERTAIN'"""
    ).fetchall():
        issue = row["issue_date"]
        expiry = row["expiry_date"]
        try:
            diff = (datetime.strptime(expiry, "%Y-%m-%d") - datetime.strptime(issue, "%Y-%m-%d")).days
            if diff < 1790 or diff > 1860:
                print(f"  {row['official_name']}: issue={issue} expiry={expiry} diff={diff}日")
        except ValueError:
            pass


# ===========================================================================
# メイン
# ===========================================================================
def main() -> None:
    print("=" * 70)
    print(" DB差分問題修正スクリプト")
    print(f" DB: {DB_PATH}")
    print("=" * 70)

    conn = get_connection()

    try:
        n1 = fix_permit_numbers(conn)
        n2 = fix_expiry_dates(conn)
        n3 = fix_company_names(conn)
        verify(conn)

        print("\n" + "=" * 70)
        print(f" 修正サマリ: 許可番号={n1}, 有効期限={n2}, 社名={n3}")
        print("=" * 70)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
