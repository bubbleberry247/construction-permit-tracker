"""
verify_permits_mlit.py — DB上の全許可証を国交省建設業者検索システムAPIでオンライン検証する。

Usage:
    python src/verify_permits_mlit.py
    python src/verify_permits_mlit.py --dry-run   # APIアクセスせずDB読み取りのみ

出力:
    C:/tmp/mlit_verification.csv  — 検証結果CSV
    コンソールにサマリー表示
"""

from __future__ import annotations

import csv
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

DB_PATH = Path(__file__).parent.parent / "data" / "permit_tracker.db"
OUTPUT_CSV = Path("C:/tmp/mlit_verification.csv")

MLIT_SEARCH_URL = "https://etsuran2.mlit.go.jp/TAKKEN/kensetuKensaku.do"
MLIT_DETAIL_URL = "https://etsuran2.mlit.go.jp/TAKKEN/ksGaiyo.do"

REQUEST_INTERVAL_SEC = 1.0  # サーバー負荷軽減

# MLIT 29業種 1文字略称（HTML表示順 = TRADE_CATEGORIES順）
MLIT_TRADE_ABBREV: list[str] = [
    "土木",      # 土
    "建築",      # 建
    "大工",      # 大
    "左官",      # 左
    "とび・土工", # と
    "石工",      # 石
    "屋根",      # 屋
    "電気",      # 電
    "管",        # 管
    "タイル・れんが・ブロック",  # 夕
    "鋼構造物",  # 鋼
    "鉄筋",      # 筋
    "舗装",      # 舗
    "しゅんせつ", # しゅ
    "板金",      # 板
    "ガラス",    # ガ
    "塗装",      # 塗
    "防水",      # 防
    "内装仕上",  # 内
    "機械器具設置", # 機
    "熱絶縁",    # 絶
    "電気通信",  # 通
    "造園",      # 園
    "さく井",    # 井
    "建具",      # 具
    "水道施設",  # 水
    "消防施設",  # 消
    "清掃施設",  # 清
    "解体",      # 解
]

# 都道府県コード（permit_authority → 2桁コード）
_PREF_CODE: dict[str, str] = {
    "北海道知事": "01", "青森県知事": "02", "岩手県知事": "03", "宮城県知事": "04",
    "秋田県知事": "05", "山形県知事": "06", "福島県知事": "07", "茨城県知事": "08",
    "栃木県知事": "09", "群馬県知事": "10", "埼玉県知事": "11", "千葉県知事": "12",
    "東京都知事": "13", "神奈川県知事": "14", "新潟県知事": "15", "富山県知事": "16",
    "石川県知事": "17", "福井県知事": "18", "山梨県知事": "19", "長野県知事": "20",
    "岐阜県知事": "21", "静岡県知事": "22", "愛知県知事": "23", "三重県知事": "24",
    "滋賀県知事": "25", "京都府知事": "26", "大阪府知事": "27", "兵庫県知事": "28",
    "奈良県知事": "29", "和歌山県知事": "30", "鳥取県知事": "31", "島根県知事": "32",
    "岡山県知事": "33", "広島県知事": "34", "山口県知事": "35", "徳島県知事": "36",
    "香川県知事": "37", "愛媛県知事": "38", "高知県知事": "39", "福岡県知事": "40",
    "佐賀県知事": "41", "長崎県知事": "42", "熊本県知事": "43", "大分県知事": "44",
    "宮崎県知事": "45", "鹿児島県知事": "46", "沖縄県知事": "47",
}

# 和暦オフセット
_WAREKI_OFFSETS: dict[str, int] = {
    "R": 2018, "H": 1988, "S": 1925, "T": 1911,
}


# ---------------------------------------------------------------------------
# データクラス
# ---------------------------------------------------------------------------

@dataclass
class PermitRecord:
    """DB上の1許可証レコード"""
    permit_id: int
    company_id: str
    official_name: str
    permit_number: str
    permit_category: str  # 般 / 特
    permit_authority: str
    issue_date: str
    expiry_date: str
    trades: str  # カンマ区切り


@dataclass
class ApiResult:
    """API取得結果"""
    found: bool = False
    api_name: str = ""
    api_expiry_wareki: str = ""
    api_expiry_iso: str = ""
    api_trades: list[str] = field(default_factory=list)
    api_category: str = ""  # 般 / 特
    error: str = ""


@dataclass
class VerificationResult:
    """検証結果"""
    permit_id: int
    company_id: str
    db_name: str
    api_name: str
    name_match: str  # OK / NG / -
    db_expiry: str
    api_expiry: str
    expiry_match: str  # OK / NG / -
    db_trades: str
    api_trades: str
    trades_match: str  # OK / NG / PARTIAL / -
    status: str  # MATCH / MISMATCH / NOT_FOUND / SKIP / ERROR


# ---------------------------------------------------------------------------
# DB読み取り
# ---------------------------------------------------------------------------

def fetch_all_permits(db_path: Path) -> list[PermitRecord]:
    """DB上の全許可証を取得する"""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
        SELECT p.permit_id, p.company_id, c.official_name, p.permit_number,
               p.permit_category, p.permit_authority, p.issue_date, p.expiry_date,
               GROUP_CONCAT(pt.trade_name, ',') as trades
        FROM permits p
        JOIN companies c ON p.company_id = c.company_id
        LEFT JOIN permit_trades pt ON p.permit_id = pt.permit_id
        GROUP BY p.permit_id
        ORDER BY p.company_id
    """)
    rows = cur.fetchall()
    conn.close()

    permits: list[PermitRecord] = []
    for r in rows:
        permits.append(PermitRecord(
            permit_id=r["permit_id"],
            company_id=r["company_id"],
            official_name=r["official_name"],
            permit_number=r["permit_number"] or "",
            permit_category=r["permit_category"] or "",
            permit_authority=r["permit_authority"] or "",
            issue_date=r["issue_date"] or "",
            expiry_date=r["expiry_date"] or "",
            trades=r["trades"] or "",
        ))
    return permits


# ---------------------------------------------------------------------------
# 許可番号 → licenseNoKbn / sv_licenseNo 変換
# ---------------------------------------------------------------------------

def get_license_no_kbn(authority: str) -> str:
    """permit_authority から licenseNoKbn を判定 (00=大臣, 01=知事)"""
    if "大臣" in authority:
        return "00"
    return "01"


def get_pref_code(authority: str) -> str | None:
    """permit_authority → 都道府県2桁コード (大臣の場合は "00")"""
    if "大臣" in authority:
        return "00"
    return _PREF_CODE.get(authority)


def normalize_permit_number(num: str) -> str:
    """許可番号から先頭ゼロや文字を除去し、純粋な数値文字列にする"""
    return num.lstrip("0") or "0"


def build_sv_license_no(pref_code: str, permit_number: str) -> str:
    """pref_code (2桁) + permit_number (6桁ゼロ埋め) = sv_licenseNo (8桁)"""
    num = normalize_permit_number(permit_number)
    return f"{pref_code}{int(num):06d}"


# ---------------------------------------------------------------------------
# 和暦パース
# ---------------------------------------------------------------------------

def wareki_period_to_iso(text: str) -> tuple[str, str]:
    """
    有効期間文字列 "R07年06月10日からR12年06月09日まで" → (開始ISO, 終了ISO)
    """
    # パターン: {元号}{年}年{月}月{日}日から{元号}{年}年{月}月{日}日まで
    pattern = re.compile(
        r"([RHST])(\d{2})年(\d{2})月(\d{2})日から"
        r"([RHST])(\d{2})年(\d{2})月(\d{2})日まで"
    )
    m = pattern.search(text)
    if not m:
        return ("", "")

    era1, y1, m1, d1, era2, y2, m2, d2 = m.groups()
    offset1 = _WAREKI_OFFSETS.get(era1, 0)
    offset2 = _WAREKI_OFFSETS.get(era2, 0)

    try:
        start = date(offset1 + int(y1), int(m1), int(d1))
        end = date(offset2 + int(y2), int(m2), int(d2))
        return (start.isoformat(), end.isoformat())
    except (ValueError, TypeError):
        return ("", "")


# ---------------------------------------------------------------------------
# MLIT API アクセス
# ---------------------------------------------------------------------------

def _build_search_params(license_no_kbn: str, permit_number: str) -> dict[str, str]:
    """検索POSTパラメータを組み立てる"""
    num = normalize_permit_number(permit_number)
    return {
        "CMD": "search", "caller": "KS", "rdoSelect": "1",
        "comNameKanaOnly": "", "comNameKanjiOnly": "", "rdoSelectJoken": "1",
        "licenseNoKbn": license_no_kbn,
        "licenseNoFrom": num,
        "licenseNoTo": num,
        "keyWord": "", "kenCode": "", "choice": "", "gyosyu": "", "gyosyuType": "",
        "sortValue": "", "rdoSelectSort": "1", "dispCount": "10", "dispPage": "1",
        "resultCount": "0", "pageCount": "0",
        "sv_rdoSelect": "", "sv_rdoSelectJoken": "", "sv_rdoSelectSort": "",
        "sv_kenCode": "", "sv_choice": "", "sv_gyosyu": "", "sv_gyosyuType": "",
        "sv_keyWord": "", "sv_sortValue": "", "sv_pageListNo1": "", "sv_pageListNo2": "",
        "sv_comNameKanaOnly": "", "sv_comNameKanjiOnly": "",
        "sv_licenseNoKbn": "", "sv_licenseNoFrom": "", "sv_licenseNoTo": "",
        "sv_licenseNo": "", "sv_dispCount": "0", "sv_dispPage": "0",
    }


def search_permit(
    session: requests.Session,
    license_no_kbn: str,
    permit_number: str,
    expected_pref_code: str | None,
) -> str | None:
    """
    検索APIを叩き、期待される都道府県コードに一致するsv_licenseNoを返す。
    見つからなければNone。
    """
    params = _build_search_params(license_no_kbn, permit_number)
    resp = session.post(MLIT_SEARCH_URL, data=params, timeout=30)
    html = resp.content.decode("cp932", errors="replace")
    soup = BeautifulSoup(html, "html.parser")

    # js_ShowDetail('XXXXXXXX') からlicense_noを抽出
    for a_tag in soup.find_all("a", onclick=re.compile(r"js_ShowDetail")):
        onclick = a_tag.get("onclick", "")
        m = re.search(r"js_ShowDetail\('(\d+)'\)", onclick)
        if m:
            license_no = m.group(1)
            # 先頭2桁がpref_codeと一致するか確認
            if expected_pref_code and license_no[:2] == expected_pref_code:
                return license_no
            # pref_codeが不明の場合は最初のヒットを返す（大臣許可は1件のみのはず）
            if not expected_pref_code and license_no_kbn == "00":
                return license_no

    return None


def fetch_detail(session: requests.Session, sv_license_no: str) -> ApiResult:
    """詳細ページを取得してパースする"""
    result = ApiResult()

    resp = session.post(
        MLIT_DETAIL_URL,
        data={"sv_licenseNo": sv_license_no, "caller": "KS"},
        timeout=30,
    )
    html = resp.content.decode("cp932", errors="replace")
    soup = BeautifulSoup(html, "html.parser")

    result.found = True

    # 商号
    for th in soup.find_all("th"):
        if "商号" in th.get_text():
            td = th.find_next_sibling("td")
            if td:
                for p in td.find_all("p", class_="phonetic"):
                    p.decompose()
                result.api_name = td.get_text(strip=True)
            break

    # 有効期間 — 最初の re_summ_5 テーブル内
    for th in soup.find_all("th"):
        th_text = th.get_text(strip=True)
        if "許可の有効期間" in th_text:
            td = th.find_next_sibling("td")
            if td:
                period_text = td.get_text(strip=True)
                result.api_expiry_wareki = period_text
                start_iso, end_iso = wareki_period_to_iso(period_text)
                result.api_expiry_iso = end_iso
            break

    # 許可業種 — re_summ_3 テーブル（上段=全業種保有一覧）
    # 2つあるうちの最初のテーブルを使用
    trade_tables = soup.find_all("table", class_="re_summ_3")
    if trade_tables:
        table = trade_tables[0]  # 上段: 全業種
        rows = table.find_all("tr")
        if len(rows) >= 2:
            data_row = rows[1]  # 2行目がデータ
            tds = data_row.find_all("td")
            for idx, td in enumerate(tds):
                val = td.get_text(strip=True)
                if val in ("1", "2") and idx < len(MLIT_TRADE_ABBREV):
                    result.api_trades.append(MLIT_TRADE_ABBREV[idx])
                    # 般/特判定（最初に見つかった業種で判定）
                    if not result.api_category:
                        result.api_category = "般" if val == "1" else "特"

    return result


# ---------------------------------------------------------------------------
# 比較ロジック
# ---------------------------------------------------------------------------

def normalize_company_name(name: str) -> str:
    """会社名の正規化（比較用）: 括弧・スペース・法人格表記を統一"""
    s = name.strip()
    # 全角→半角変換（括弧）
    s = s.replace("（", "(").replace("）", ")")
    # 株式会社 → (株)
    s = s.replace("株式会社", "(株)")
    # 有限会社 → (有)
    s = s.replace("有限会社", "(有)")
    # スペース除去
    s = s.replace(" ", "").replace("　", "")
    return s


def normalize_trade_name(trade: str) -> str:
    """業種名正規化: 「工事業」「工事」を除去して基幹部分だけ比較"""
    s = trade.strip()
    s = s.replace("工事業", "").replace("工事", "")
    return s


def compare_trades(db_trades_str: str, api_trades: list[str]) -> str:
    """
    DB業種とAPI業種を比較する。

    Returns: "OK" / "PARTIAL" / "NG" / "-"
    """
    if not db_trades_str and not api_trades:
        return "OK"
    if not db_trades_str or not api_trades:
        return "NG"

    # DB側: カンマ区切り → 正規化セット
    db_set = {normalize_trade_name(t) for t in db_trades_str.split(",") if t.strip()}
    api_set = {normalize_trade_name(t) for t in api_trades}

    if db_set == api_set:
        return "OK"
    # DB側がAPI側の部分集合なら PARTIAL（同一許可番号で般/特に分かれている場合）
    if db_set.issubset(api_set):
        return "PARTIAL"
    if api_set.issubset(db_set):
        return "PARTIAL"
    # 共通部分があるか
    if db_set & api_set:
        return "PARTIAL"
    return "NG"


def compare_expiry(db_expiry: str, api_expiry_iso: str) -> str:
    """有効期限の比較"""
    if not db_expiry or not api_expiry_iso:
        return "-"
    if db_expiry == api_expiry_iso:
        return "OK"
    return "NG"


def compare_name(db_name: str, api_name: str) -> str:
    """会社名の比較"""
    if not db_name or not api_name:
        return "-"
    n1 = normalize_company_name(db_name)
    n2 = normalize_company_name(api_name)
    if n1 == n2:
        return "OK"
    # 部分一致チェック（一方がもう一方に含まれる）
    if n1 in n2 or n2 in n1:
        return "OK"
    return "NG"


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def verify_permit(
    session: requests.Session,
    permit: PermitRecord,
) -> VerificationResult:
    """1件の許可証をAPIで検証する"""

    # permit_number / permit_authority がないものはスキップ
    if not permit.permit_number or not permit.permit_authority:
        return VerificationResult(
            permit_id=permit.permit_id,
            company_id=permit.company_id,
            db_name=permit.official_name,
            api_name="",
            name_match="-",
            db_expiry=permit.expiry_date,
            api_expiry="",
            expiry_match="-",
            db_trades=permit.trades,
            api_trades="",
            trades_match="-",
            status="SKIP",
        )

    license_no_kbn = get_license_no_kbn(permit.permit_authority)
    pref_code = get_pref_code(permit.permit_authority)

    try:
        # Step 1: 検索
        sv_license_no = search_permit(
            session, license_no_kbn, permit.permit_number, pref_code,
        )

        if not sv_license_no:
            return VerificationResult(
                permit_id=permit.permit_id,
                company_id=permit.company_id,
                db_name=permit.official_name,
                api_name="",
                name_match="-",
                db_expiry=permit.expiry_date,
                api_expiry="",
                expiry_match="-",
                db_trades=permit.trades,
                api_trades="",
                trades_match="-",
                status="NOT_FOUND",
            )

        time.sleep(REQUEST_INTERVAL_SEC)

        # Step 2: 詳細取得
        detail = fetch_detail(session, sv_license_no)

        # Step 3: 比較
        name_match = compare_name(permit.official_name, detail.api_name)
        expiry_match = compare_expiry(permit.expiry_date, detail.api_expiry_iso)
        trades_match = compare_trades(permit.trades, detail.api_trades)

        api_trades_str = "、".join(detail.api_trades) if detail.api_trades else ""

        # 全一致ならMATCH, 一つでもNGならMISMATCH
        if name_match == "OK" and expiry_match == "OK" and trades_match in ("OK", "PARTIAL"):
            status = "MATCH"
        elif "NG" in (name_match, expiry_match, trades_match):
            status = "MISMATCH"
        else:
            status = "PARTIAL"

        return VerificationResult(
            permit_id=permit.permit_id,
            company_id=permit.company_id,
            db_name=permit.official_name,
            api_name=detail.api_name,
            name_match=name_match,
            db_expiry=permit.expiry_date,
            api_expiry=detail.api_expiry_iso,
            expiry_match=expiry_match,
            db_trades=permit.trades,
            api_trades=api_trades_str,
            trades_match=trades_match,
            status=status,
        )

    except Exception as exc:
        return VerificationResult(
            permit_id=permit.permit_id,
            company_id=permit.company_id,
            db_name=permit.official_name,
            api_name="",
            name_match="-",
            db_expiry=permit.expiry_date,
            api_expiry="",
            expiry_match="-",
            db_trades=permit.trades,
            api_trades="",
            trades_match="-",
            status=f"ERROR: {exc}",
        )


def write_csv(results: list[VerificationResult], output_path: Path) -> None:
    """検証結果をCSV出力する"""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    headers = [
        "permit_id", "company_id", "db_name", "api_name", "name_match",
        "db_expiry", "api_expiry", "expiry_match",
        "db_trades", "api_trades", "trades_match", "status",
    ]

    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for r in results:
            writer.writerow({
                "permit_id": r.permit_id,
                "company_id": r.company_id,
                "db_name": r.db_name,
                "api_name": r.api_name,
                "name_match": r.name_match,
                "db_expiry": r.db_expiry,
                "api_expiry": r.api_expiry,
                "expiry_match": r.expiry_match,
                "db_trades": r.db_trades,
                "api_trades": r.api_trades,
                "trades_match": r.trades_match,
                "status": r.status,
            })

    print(f"\n[CSV] {output_path} に {len(results)} 件出力しました")


def print_summary(results: list[VerificationResult]) -> None:
    """サマリーを表示する"""
    total = len(results)
    skip = sum(1 for r in results if r.status == "SKIP")
    searched = total - skip
    found = sum(1 for r in results if r.status not in ("SKIP", "NOT_FOUND") and not r.status.startswith("ERROR"))
    not_found = sum(1 for r in results if r.status == "NOT_FOUND")
    errors = sum(1 for r in results if r.status.startswith("ERROR"))
    match = sum(1 for r in results if r.status == "MATCH")
    partial = sum(1 for r in results if r.status == "PARTIAL")
    mismatch = sum(1 for r in results if r.status == "MISMATCH")

    name_ok = sum(1 for r in results if r.name_match == "OK")
    name_ng = sum(1 for r in results if r.name_match == "NG")
    expiry_ok = sum(1 for r in results if r.expiry_match == "OK")
    expiry_ng = sum(1 for r in results if r.expiry_match == "NG")
    trades_ok = sum(1 for r in results if r.trades_match == "OK")
    trades_partial = sum(1 for r in results if r.trades_match == "PARTIAL")
    trades_ng = sum(1 for r in results if r.trades_match == "NG")

    print("\n" + "=" * 60)
    print("  MLIT オンライン検証サマリー")
    print("=" * 60)
    print(f"  DB許可証:      {total} 件")
    print(f"  スキップ:      {skip} 件 (番号/authority未設定)")
    print(f"  検索実行:      {searched} 件")
    print(f"  ├ 検索ヒット:  {found} 件 ({found/searched*100:.0f}%)" if searched > 0 else "")
    print(f"  ├ 検索不一致:  {not_found} 件" if not_found > 0 else "")
    print(f"  └ エラー:      {errors} 件" if errors > 0 else "")
    print()
    print(f"  [完全一致]     {match} 件")
    print(f"  [部分一致]     {partial} 件")
    print(f"  [不一致]       {mismatch} 件")
    print()
    print(f"  名前一致: OK={name_ok} NG={name_ng}")
    print(f"  期限一致: OK={expiry_ok} NG={expiry_ng}")
    print(f"  業種一致: OK={trades_ok} PARTIAL={trades_partial} NG={trades_ng}")

    # 不一致一覧
    mismatches = [r for r in results if r.status in ("MISMATCH", "NOT_FOUND") or r.status.startswith("ERROR")]
    if mismatches:
        print()
        print("-" * 60)
        print("  不一致・エラー一覧")
        print("-" * 60)
        for r in mismatches:
            print(f"  permit_id={r.permit_id} ({r.company_id}) {r.db_name}")
            print(f"    status={r.status}")
            if r.name_match == "NG":
                print(f"    名前: DB=[{r.db_name}] API=[{r.api_name}]")
            if r.expiry_match == "NG":
                print(f"    期限: DB=[{r.db_expiry}] API=[{r.api_expiry}]")
            if r.trades_match in ("NG", "PARTIAL"):
                print(f"    業種: DB=[{r.db_trades}]")
                print(f"          API=[{r.api_trades}]")
            print()

    print("=" * 60)


def main(dry_run: bool = False) -> None:
    """メイン処理"""
    print("=" * 60)
    print("  MLIT 建設業者検索システム オンライン検証")
    print("=" * 60)

    # Step 1: DB読み取り
    permits = fetch_all_permits(DB_PATH)
    print(f"\n[DB] {len(permits)} 件の許可証を取得しました")

    if dry_run:
        print("\n[DRY-RUN] APIアクセスをスキップします")
        for p in permits:
            kbn = get_license_no_kbn(p.permit_authority) if p.permit_authority else "?"
            pref = get_pref_code(p.permit_authority) or "?"
            num = p.permit_number or "(empty)"
            print(f"  permit_id={p.permit_id} {p.company_id} kbn={kbn} pref={pref} num={num} [{p.official_name}]")
        return

    # Step 2-4: API検索 → 詳細取得 → 比較
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Content-Type": "application/x-www-form-urlencoded",
    })

    results: list[VerificationResult] = []

    for i, permit in enumerate(permits):
        print(
            f"[{i+1}/{len(permits)}] "
            f"permit_id={permit.permit_id} "
            f"{permit.company_id} "
            f"{permit.official_name} "
            f"num={permit.permit_number} "
            f"auth={permit.permit_authority}",
            end=" ... ",
            flush=True,
        )

        result = verify_permit(session, permit)
        results.append(result)
        print(result.status)

        # リクエスト間隔
        if i < len(permits) - 1 and result.status != "SKIP":
            time.sleep(REQUEST_INTERVAL_SEC)

    # Step 5: CSV出力
    write_csv(results, OUTPUT_CSV)

    # Step 6: サマリー表示
    print_summary(results)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="MLIT オンライン検証")
    parser.add_argument("--dry-run", action="store_true", help="APIアクセスせずDB確認のみ")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
