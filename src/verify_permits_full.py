"""
verify_permits_full.py -- DB全permits を国交省建設業者検索システムAPIで検証し、DB値と照合する。

検索API (kensetuKensaku.do) で sv_licenseNo を取得し、
詳細API (ksGaiyo.do) で商号・有効期間・業種を取得して比較する。

Usage:
    python src/verify_permits_full.py
    python src/verify_permits_full.py --dry-run

出力:
    C:/tmp/mlit_full_verification.csv  -- 全件検証結果CSV
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

import requests
from bs4 import BeautifulSoup

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

DB_PATH = Path(__file__).parent.parent / "data" / "permit_tracker.db"
OUTPUT_CSV = Path("C:/tmp/mlit_full_verification.csv")

MLIT_SEARCH_URL = "https://etsuran2.mlit.go.jp/TAKKEN/kensetuKensaku.do"
MLIT_DETAIL_URL = "https://etsuran2.mlit.go.jp/TAKKEN/ksGaiyo.do"

REQUEST_INTERVAL_SEC = 1.0

# MLIT 29業種 略称（HTML表示順）
MLIT_TRADE_ABBREV: list[str] = [
    "土木", "建築", "大工", "左官", "とび・土工",
    "石工", "屋根", "電気", "管", "タイル・れんが・ブロック",
    "鋼構造物", "鉄筋", "舗装", "しゅんせつ", "板金",
    "ガラス", "塗装", "防水", "内装仕上", "機械器具設置",
    "熱絶縁", "電気通信", "造園", "さく井", "建具",
    "水道施設", "消防施設", "清掃施設", "解体",
]

# 正式名称(工事業付き) -> 略称
FORMAL_TO_ABBREV: dict[str, str] = {
    "土木工事業": "土木",
    "建築工事業": "建築",
    "大工工事業": "大工",
    "左官工事業": "左官",
    "とび・土工工事業": "とび・土工",
    "石工事業": "石工",
    "屋根工事業": "屋根",
    "電気工事業": "電気",
    "管工事業": "管",
    "タイル・れんが・ブロック工事業": "タイル・れんが・ブロック",
    "鋼構造物工事業": "鋼構造物",
    "鉄筋工事業": "鉄筋",
    "舗装工事業": "舗装",
    "しゅんせつ工事業": "しゅんせつ",
    "板金工事業": "板金",
    "ガラス工事業": "ガラス",
    "塗装工事業": "塗装",
    "防水工事業": "防水",
    "内装仕上工事業": "内装仕上",
    "機械器具設置工事業": "機械器具設置",
    "熱絶縁工事業": "熱絶縁",
    "電気通信工事業": "電気通信",
    "造園工事業": "造園",
    "さく井工事業": "さく井",
    "建具工事業": "建具",
    "水道施設工事業": "水道施設",
    "消防施設工事業": "消防施設",
    "清掃施設工事業": "清掃施設",
    "解体工事業": "解体",
    # DB上で「消防施設」「電気」等の略称のみで入っているケースもある
    "消防施設": "消防施設",
    "電気": "電気",
}

# 都道府県コード
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

_WAREKI_OFFSETS: dict[str, int] = {
    "R": 2018, "H": 1988, "S": 1925, "T": 1911,
}


# ---------------------------------------------------------------------------
# データクラス
# ---------------------------------------------------------------------------

@dataclass
class PermitRecord:
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
class ApiDetail:
    found: bool = False
    sv_license_no: str = ""
    api_name: str = ""
    api_expiry_from: str = ""
    api_expiry_to: str = ""
    api_expiry_wareki: str = ""
    api_trades_ippan: list[str] = field(default_factory=list)
    api_trades_tokutei: list[str] = field(default_factory=list)
    error: str = ""


@dataclass
class VerificationRow:
    permit_id: int
    company_id: str
    db_name: str
    api_name: str
    name_match: str
    db_expiry: str
    api_expiry_from: str
    api_expiry_to: str
    expiry_match: str
    db_trades: str
    api_trades: str
    trades_match: str
    issues: str


# ---------------------------------------------------------------------------
# DB読み取り
# ---------------------------------------------------------------------------

def fetch_all_permits(db_path: Path) -> list[PermitRecord]:
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

    return [
        PermitRecord(
            permit_id=r["permit_id"],
            company_id=r["company_id"],
            official_name=r["official_name"],
            permit_number=r["permit_number"] or "",
            permit_category=r["permit_category"] or "",
            permit_authority=r["permit_authority"] or "",
            issue_date=r["issue_date"] or "",
            expiry_date=r["expiry_date"] or "",
            trades=r["trades"] or "",
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# 許可番号変換
# ---------------------------------------------------------------------------

def get_license_no_kbn(authority: str) -> str:
    """大臣=00, 知事=01"""
    return "00" if "大臣" in authority else "01"


def get_pref_code(authority: str) -> str | None:
    if "大臣" in authority:
        return "00"
    return _PREF_CODE.get(authority)


def normalize_permit_number(num: str) -> str:
    return num.lstrip("0") or "0"


# ---------------------------------------------------------------------------
# 和暦パース
# ---------------------------------------------------------------------------

def wareki_period_to_iso(text: str) -> tuple[str, str]:
    pattern = re.compile(
        r"([RHST])(\d{2})年(\d{2})月(\d{2})日から"
        r"([RHST])(\d{2})年(\d{2})月(\d{2})日まで"
    )
    m = pattern.search(text)
    if not m:
        return ("", "")
    era1, y1, m1, d1, era2, y2, m2, d2 = m.groups()
    off1 = _WAREKI_OFFSETS.get(era1, 0)
    off2 = _WAREKI_OFFSETS.get(era2, 0)
    try:
        start = date(off1 + int(y1), int(m1), int(d1))
        end = date(off2 + int(y2), int(m2), int(d2))
        return (start.isoformat(), end.isoformat())
    except (ValueError, TypeError):
        return ("", "")


# ---------------------------------------------------------------------------
# MLIT API
# ---------------------------------------------------------------------------

def _build_search_params(license_no_kbn: str, permit_number: str) -> dict[str, str]:
    num = normalize_permit_number(permit_number)
    return {
        "CMD": "search", "caller": "KS", "rdoSelect": "1",
        "comNameKanaOnly": "", "comNameKanjiOnly": "", "rdoSelectJoken": "1",
        "licenseNoKbn": license_no_kbn,
        "licenseNoFrom": num, "licenseNoTo": num,
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


def _post_with_retry(
    session: requests.Session,
    url: str,
    data: dict[str, str],
    max_retries: int = 5,
    retry_wait: float = 30.0,
) -> requests.Response:
    """POST with retry on 5xx errors"""
    for attempt in range(max_retries):
        resp = session.post(url, data=data, timeout=60)
        if resp.status_code < 500:
            return resp
        wait = retry_wait * (attempt + 1)
        print(f"\n  [RETRY] {resp.status_code} - waiting {wait:.0f}s (attempt {attempt+1}/{max_retries})", end="", flush=True)
        time.sleep(wait)
    return resp  # return last response even if still 5xx


def search_permit(
    session: requests.Session,
    license_no_kbn: str,
    permit_number: str,
    expected_pref_code: str | None,
) -> str | None:
    """検索APIで sv_licenseNo を取得する"""
    params = _build_search_params(license_no_kbn, permit_number)
    resp = _post_with_retry(session, MLIT_SEARCH_URL, params)
    if resp.status_code >= 500:
        return None
    html = resp.content.decode("cp932", errors="replace")
    soup = BeautifulSoup(html, "html.parser")

    for a_tag in soup.find_all("a", onclick=re.compile(r"js_ShowDetail")):
        onclick = a_tag.get("onclick", "")
        m = re.search(r"js_ShowDetail\('(\d+)'\)", onclick)
        if m:
            license_no = m.group(1)
            if expected_pref_code and license_no[:2] == expected_pref_code:
                return license_no
            if not expected_pref_code and license_no_kbn == "00":
                return license_no
    return None


def fetch_detail(session: requests.Session, sv_license_no: str) -> ApiDetail:
    """詳細ページを取得してパースする（般/特の業種を分離）"""
    result = ApiDetail(sv_license_no=sv_license_no)

    try:
        resp = _post_with_retry(
            session, MLIT_DETAIL_URL,
            {"sv_licenseNo": sv_license_no, "caller": "KS"},
        )
        if resp.status_code >= 500:
            result.found = False
            result.error = f"HTTP {resp.status_code}"
            return result
        html = resp.content.decode("cp932", errors="replace")
        soup = BeautifulSoup(html, "html.parser")

        # 商号
        found_name = False
        for th in soup.find_all("th"):
            if "商号" in th.get_text():
                td = th.find_next_sibling("td")
                if td:
                    for p in td.find_all("p", class_="phonetic"):
                        p.decompose()
                    result.api_name = td.get_text(strip=True)
                    found_name = True
                break

        if not found_name:
            result.found = False
            result.error = "商号が見つからない"
            return result

        result.found = True

        # 有効期間
        for th in soup.find_all("th"):
            if "許可の有効期間" in th.get_text(strip=True):
                td = th.find_next_sibling("td")
                if td:
                    period_text = td.get_text(strip=True)
                    result.api_expiry_wareki = period_text
                    start_iso, end_iso = wareki_period_to_iso(period_text)
                    result.api_expiry_from = start_iso
                    result.api_expiry_to = end_iso
                break

        # 許可業種 -- re_summ_3 テーブル
        # 上段: 全業種（1=般, 2=特）
        trade_tables = soup.find_all("table", class_="re_summ_3")
        if trade_tables:
            table = trade_tables[0]
            rows = table.find_all("tr")
            if len(rows) >= 2:
                data_row = rows[1]
                tds = data_row.find_all("td")
                for idx, td in enumerate(tds):
                    val = td.get_text(strip=True)
                    if idx < len(MLIT_TRADE_ABBREV):
                        if val == "1":
                            result.api_trades_ippan.append(MLIT_TRADE_ABBREV[idx])
                        elif val == "2":
                            result.api_trades_tokutei.append(MLIT_TRADE_ABBREV[idx])

    except Exception as exc:
        result.found = False
        result.error = str(exc)

    return result


# ---------------------------------------------------------------------------
# 比較ロジック
# ---------------------------------------------------------------------------

def normalize_company_name(name: str) -> str:
    s = name.strip()
    s = s.replace("（", "(").replace("）", ")")
    s = s.replace("株式会社", "(株)").replace("有限会社", "(有)")
    s = s.replace(" ", "").replace("\u3000", "")
    return s


def compare_name(db_name: str, api_name: str) -> tuple[str, str]:
    if not db_name or not api_name:
        return ("-", "")
    n1 = normalize_company_name(db_name)
    n2 = normalize_company_name(api_name)
    if n1 == n2 or n1 in n2 or n2 in n1:
        return ("OK", "")
    return ("NG", f"名前不一致: DB=[{db_name}] API=[{api_name}]")


def compare_expiry(db_expiry: str, api_expiry_to: str) -> tuple[str, str]:
    if not db_expiry or not api_expiry_to:
        return ("-", "")
    if db_expiry == api_expiry_to:
        return ("OK", "")
    return ("NG", f"期限不一致: DB=[{db_expiry}] API=[{api_expiry_to}]")


def normalize_trade(trade: str) -> str:
    """DB業種名を略称に変換"""
    t = trade.strip()
    if t in FORMAL_TO_ABBREV:
        return FORMAL_TO_ABBREV[t]
    # 「工事業」「工事」を除去して略称マッチ
    t2 = t.replace("工事業", "").replace("工事", "")
    for abbrev in MLIT_TRADE_ABBREV:
        if t2 == abbrev:
            return abbrev
    # 直接一致する略称があればそれを使う
    if t in MLIT_TRADE_ABBREV:
        return t
    return t


def compare_trades(
    db_trades_str: str,
    permit_category: str,
    api_trades_ippan: list[str],
    api_trades_tokutei: list[str],
) -> tuple[str, str]:
    """
    般/特それぞれで比較。
    DB側の業種がAPI側の対応カテゴリに含まれていればOK。
    """
    if not db_trades_str:
        return ("-", "")

    db_trades = {normalize_trade(t) for t in db_trades_str.split(",") if t.strip()}

    if permit_category == "般":
        api_trades = set(api_trades_ippan)
    elif permit_category == "特":
        api_trades = set(api_trades_tokutei)
    else:
        api_trades = set(api_trades_ippan) | set(api_trades_tokutei)

    if not api_trades and not db_trades:
        return ("OK", "")
    if not api_trades:
        return ("NG", f"API側に{permit_category}業種なし (DB: {','.join(sorted(db_trades))})")

    if db_trades == api_trades or db_trades.issubset(api_trades):
        return ("OK", "")

    missing = db_trades - api_trades
    extra = api_trades - db_trades

    if missing:
        parts = []
        parts.append(f"DBにあるがAPIにない: {','.join(sorted(missing))}")
        if extra:
            parts.append(f"APIにあるがDBにない: {','.join(sorted(extra))}")
        return ("NG", "; ".join(parts))

    return ("OK", "")


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

def make_skip_row(permit: PermitRecord, reason: str) -> VerificationRow:
    return VerificationRow(
        permit_id=permit.permit_id,
        company_id=permit.company_id,
        db_name=permit.official_name,
        api_name="",
        name_match="-",
        db_expiry=permit.expiry_date,
        api_expiry_from="",
        api_expiry_to="",
        expiry_match="-",
        db_trades=permit.trades,
        api_trades="",
        trades_match="-",
        issues=reason,
    )


def verify_all(dry_run: bool = False) -> None:
    print("=" * 70)
    print("  国交省 建設業者検索システム 全件検証")
    print("=" * 70)

    permits = fetch_all_permits(DB_PATH)
    print(f"\n[DB] {len(permits)} 件の許可証を取得しました")

    if dry_run:
        print("\n[DRY-RUN] APIアクセスをスキップします")
        for p in permits:
            kbn = get_license_no_kbn(p.permit_authority) if p.permit_authority else "?"
            pref = get_pref_code(p.permit_authority) or "?"
            print(f"  permit_id={p.permit_id} {p.company_id} [{p.permit_category}] "
                  f"kbn={kbn} pref={pref} num={p.permit_number} [{p.official_name}]")
        return

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Content-Type": "application/x-www-form-urlencoded",
    })

    # サーバー疎通チェック（復旧するまで待機）
    print("\n[WARMUP] サーバー疎通チェック中...", flush=True)
    for attempt in range(60):  # 最大30分待機
        try:
            resp = session.get(MLIT_SEARCH_URL, timeout=15)
            if resp.status_code < 500:
                print(f" OK (HTTP {resp.status_code})")
                break
            print(f" HTTP {resp.status_code} - 30秒後にリトライ ({attempt+1}/60)", flush=True)
        except Exception as e:
            print(f" {e} - 30秒後にリトライ ({attempt+1}/60)", flush=True)
        time.sleep(30)
    else:
        print("\n[ERROR] サーバーが30分以上応答しません。中断します。")
        return

    # 同一 permit_number+authority のキャッシュ
    detail_cache: dict[str, ApiDetail] = {}

    results: list[VerificationRow] = []

    for i, permit in enumerate(permits):
        label = (
            f"[{i+1}/{len(permits)}] "
            f"permit_id={permit.permit_id} {permit.company_id} "
            f"{permit.official_name} [{permit.permit_category}] "
            f"num={permit.permit_number}"
        )
        print(label, end=" ... ", flush=True)

        if not permit.permit_number or not permit.permit_authority:
            results.append(make_skip_row(permit, "SKIP: permit_number or authority empty"))
            print("SKIP")
            continue

        # キャッシュキー: authority + permit_number
        cache_key = f"{permit.permit_authority}|{permit.permit_number}"

        if cache_key in detail_cache:
            detail = detail_cache[cache_key]
            print(f"[cached sv={detail.sv_license_no}]", end=" ")
        else:
            # Step 2: 検索APIで sv_licenseNo 取得
            license_no_kbn = get_license_no_kbn(permit.permit_authority)
            pref_code = get_pref_code(permit.permit_authority)

            try:
                sv = search_permit(session, license_no_kbn, permit.permit_number, pref_code)
            except Exception as exc:
                results.append(make_skip_row(permit, f"ERROR(search): {exc}"))
                print(f"ERROR(search)")
                time.sleep(REQUEST_INTERVAL_SEC)
                continue

            time.sleep(REQUEST_INTERVAL_SEC)

            if not sv:
                detail = ApiDetail(found=False, error="検索ヒットなし")
                detail_cache[cache_key] = detail
            else:
                # Step 3: 詳細取得
                try:
                    detail = fetch_detail(session, sv)
                except Exception as exc:
                    detail = ApiDetail(found=False, error=f"詳細取得エラー: {exc}")

                detail_cache[cache_key] = detail
                time.sleep(REQUEST_INTERVAL_SEC)

            print(f"sv={sv or 'N/A'}", end=" ")

        if not detail.found:
            results.append(make_skip_row(permit, f"NOT_FOUND: {detail.error}"))
            print("NOT_FOUND")
            continue

        # Step 4: 比較
        name_match, name_issue = compare_name(permit.official_name, detail.api_name)
        expiry_match, expiry_issue = compare_expiry(permit.expiry_date, detail.api_expiry_to)
        trades_match, trades_issue = compare_trades(
            permit.trades, permit.permit_category,
            detail.api_trades_ippan, detail.api_trades_tokutei,
        )

        api_trades_parts = []
        if detail.api_trades_ippan:
            api_trades_parts.append(f"般:{','.join(detail.api_trades_ippan)}")
        if detail.api_trades_tokutei:
            api_trades_parts.append(f"特:{','.join(detail.api_trades_tokutei)}")
        api_trades_str = " / ".join(api_trades_parts)

        issues_list = [x for x in [name_issue, expiry_issue, trades_issue] if x]
        issues_str = "; ".join(issues_list) if issues_list else "OK"

        row = VerificationRow(
            permit_id=permit.permit_id,
            company_id=permit.company_id,
            db_name=permit.official_name,
            api_name=detail.api_name,
            name_match=name_match,
            db_expiry=permit.expiry_date,
            api_expiry_from=detail.api_expiry_from,
            api_expiry_to=detail.api_expiry_to,
            expiry_match=expiry_match,
            db_trades=permit.trades,
            api_trades=api_trades_str,
            trades_match=trades_match,
            issues=issues_str,
        )
        results.append(row)
        print("OK" if issues_str == "OK" else f"MISMATCH [{issues_str[:60]}]")

    # Step 5: CSV
    write_csv(results)

    # Step 6: サマリー
    print_summary(results)


def write_csv(results: list[VerificationRow]) -> None:
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    headers = [
        "permit_id", "company_id", "db_name", "api_name", "name_match",
        "db_expiry", "api_expiry_from", "api_expiry_to", "expiry_match",
        "db_trades", "api_trades", "trades_match", "issues",
    ]
    with OUTPUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
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
                "api_expiry_from": r.api_expiry_from,
                "api_expiry_to": r.api_expiry_to,
                "expiry_match": r.expiry_match,
                "db_trades": r.db_trades,
                "api_trades": r.api_trades,
                "trades_match": r.trades_match,
                "issues": r.issues,
            })
    print(f"\n[CSV] {OUTPUT_CSV} に {len(results)} 件出力しました")


def print_summary(results: list[VerificationRow]) -> None:
    total = len(results)
    skip = sum(1 for r in results if r.issues.startswith("SKIP"))
    not_found = sum(1 for r in results if r.issues.startswith("NOT_FOUND"))
    ok = sum(1 for r in results if r.issues == "OK")
    mismatch = [
        r for r in results
        if r.issues != "OK"
        and not r.issues.startswith("SKIP")
        and not r.issues.startswith("NOT_FOUND")
        and not r.issues.startswith("ERROR")
    ]
    errors = sum(1 for r in results if r.issues.startswith("ERROR"))

    name_ng = sum(1 for r in results if r.name_match == "NG")
    expiry_ng = sum(1 for r in results if r.expiry_match == "NG")
    trades_ng = sum(1 for r in results if r.trades_match == "NG")

    print("\n" + "=" * 70)
    print("  国交省API全件検証サマリー")
    print("=" * 70)
    print(f"  DB許可証:      {total} 件")
    print(f"  スキップ:      {skip} 件")
    print(f"  NOT_FOUND:     {not_found} 件")
    print(f"  エラー:        {errors} 件")
    print(f"  完全一致:      {ok} 件")
    print(f"  差異あり:      {len(mismatch)} 件")
    print(f"    - 名前NG:     {name_ng} 件")
    print(f"    - 期限NG:     {expiry_ng} 件")
    print(f"    - 業種NG:     {trades_ng} 件")

    if mismatch:
        print()
        print("-" * 70)
        print("  差異の詳細一覧")
        print("-" * 70)
        for r in mismatch:
            print(f"  permit_id={r.permit_id} {r.company_id} {r.db_name} [{r.issues}]")
            if r.name_match == "NG":
                print(f"    DB名: {r.db_name}")
                print(f"    API名: {r.api_name}")
            if r.expiry_match == "NG":
                print(f"    DB期限: {r.db_expiry}")
                print(f"    API期限: {r.api_expiry_from} ~ {r.api_expiry_to}")
            if r.trades_match == "NG":
                print(f"    DB業種: {r.db_trades}")
                print(f"    API業種: {r.api_trades}")
            print()

    if not_found:
        print()
        print("-" * 70)
        print("  NOT_FOUND 一覧")
        print("-" * 70)
        for r in results:
            if r.issues.startswith("NOT_FOUND"):
                print(f"  permit_id={r.permit_id} {r.company_id} {r.db_name}")

    print("=" * 70)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="国交省API全件検証")
    parser.add_argument("--dry-run", action="store_true", help="APIアクセスせずDB確認のみ")
    args = parser.parse_args()
    verify_all(dry_run=args.dry_run)
