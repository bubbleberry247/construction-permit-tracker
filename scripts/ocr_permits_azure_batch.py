"""
Azure DI prebuilt-layout で建設業許可証PDFを一括OCR→正規表現抽出→staging_permits CSV出力。

GPT Vision 不使用。純粋にAzure DI単体のOCRテキストから建設業許可証の構造化フィールドを抽出する。

Usage:
    op run --env-file config/azure_di.env.op -- python scripts/ocr_permits_azure_batch.py
"""
from __future__ import annotations

import csv
import json
import os
import re
import sys
from datetime import date, datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
STAGING = PROJECT_ROOT / "data" / "staging"
OUTPUT = PROJECT_ROOT / "output"
OUTPUT.mkdir(exist_ok=True)

PERMIT_KEYWORDS = ["建設業許可", "建築業許可", "許可証", "許可通知"]

# ---------------------------------------------------------------------------
# 正規表現ルール（建設業許可証テンプレ）
# ---------------------------------------------------------------------------
RE_PERMIT_NUM = re.compile(
    r"(?P<auth>[^\s]+?(?:知事|大臣))\s*許\s*可\s*[（(]\s*(?P<cat>般|特)\s*[–\-―—ー]+\s*(?P<year>\d+)\s*[）)]\s*第\s*(?P<num>\d+)\s*号"
)
RE_JP_DATE = re.compile(r"(?P<era>令和|平成|昭和)?\s*(?P<y>\d+)\s*年\s*(?P<m>\d+)\s*月\s*(?P<d>\d+)\s*日")
RE_EXPIRY_HINT = re.compile(r"有\s*効\s*期\s*(?:限|間)")
RE_ISSUE_HINT = re.compile(r"許\s*可\s*年\s*月\s*日")
RE_RANGE_DATE = re.compile(
    r"(?P<era1>令和|平成|昭和)?\s*(?P<y1>\d+)\s*年\s*(?P<m1>\d+)\s*月\s*(?P<d1>\d+)\s*日\s*から\s*"
    r"(?P<era2>令和|平成|昭和)?\s*(?P<y2>\d+)\s*年\s*(?P<m2>\d+)\s*月\s*(?P<d2>\d+)\s*日\s*まで"
)
# 和暦「年」だけの行（「令和 3年」）
RE_ERA_YEAR_ONLY = re.compile(r"^\s*(?P<era>令和|平成|昭和)\s*(?P<y>\d+)\s*年\s*$")
# 月日のみ（「4月14日」）
RE_MONTH_DAY_ONLY = re.compile(r"^\s*(?P<m>\d+)\s*月\s*(?P<d>\d+)\s*日\s*$")
# 証明書判定
IS_CERTIFICATE = re.compile(r"建\s*設\s*業\s*許\s*可\s*証\s*明\s*書|ことを\s*証\s*明\s*します")

# 業種マスタ（29業種）
TRADES = {
    "土木工事業", "建築工事業", "大工工事業", "左官工事業", "とび・土工工事業",
    "とび・土工・コンクリート工事業", "石工事業", "屋根工事業", "電気工事業",
    "管工事業", "タイル・れんが・ブロック工事業", "鋼構造物工事業", "鉄筋工事業",
    "舗装工事業", "しゅんせつ工事業", "板金工事業", "ガラス工事業", "塗装工事業",
    "防水工事業", "内装仕上工事業", "機械器具設置工事業", "熱絶縁工事業",
    "電気通信工事業", "造園工事業", "さく井工事業", "建具工事業", "水道施設工事業",
    "消防施設工事業", "清掃施設工事業", "解体工事業",
}


def jp_to_iso(year: str, month: str, day: str, era: str = "令和") -> str:
    """和暦 → ISO 変換（令和/平成/昭和）"""
    base = {"令和": 2018, "平成": 1988, "昭和": 1925}.get(era, 2018)
    try:
        y = base + int(year)
        m = int(month)
        d = int(day)
        return date(y, m, d).isoformat()
    except Exception:
        return ""


def extract_fields(full_text: str) -> dict:
    out = {
        "company_name_raw": "",
        "permit_authority_name": "",
        "permit_category": "",
        "permit_year": "",
        "permit_number_full": "",
        "contractor_number": "",
        "issue_date": "",
        "expiry_date": "",
        "trade_categories": "",
        "parse_status": "ERROR",
        "error_reason": "",
    }

    # 許可番号
    m = RE_PERMIT_NUM.search(full_text)
    if m:
        out["permit_authority_name"] = m.group("auth")
        out["permit_category"] = "一般" if m.group("cat") == "般" else "特定"
        out["permit_year"] = m.group("year")
        out["contractor_number"] = m.group("num")
        out["permit_number_full"] = (
            f"{m.group('auth')} 許可（{m.group('cat')}-{m.group('year')}）第{m.group('num')}号"
        )

    # 日付抽出:
    # パターン1（通知書）: "令和X年Y月Z日から令和A年B月C日まで"
    # パターン2（許可証テーブル）: 「許可年月日」「有効期限」キーワード近傍の和暦（複数行分断対応）
    # パターン3（証明書）: 「ことを証明します」→ 有効期限なし。許可年月日のみ抽出
    lines = full_text.split("\n")
    is_cert = bool(IS_CERTIFICATE.search(full_text))

    # パターン1: 範囲日付
    for window in [full_text, full_text.replace("\n", " ")]:
        m_range = RE_RANGE_DATE.search(window)
        if m_range:
            era1 = m_range.group("era1") or "令和"
            era2 = m_range.group("era2") or era1
            out["issue_date"] = jp_to_iso(m_range.group("y1"), m_range.group("m1"), m_range.group("d1"), era1)
            out["expiry_date"] = jp_to_iso(m_range.group("y2"), m_range.group("m2"), m_range.group("d2"), era2)
            break

    # パターン2: 許可証テーブル
    # サブパターン2a: header/date 交互「許可年月日 → 日付 → 有効期限 → 日付」
    # サブパターン2b: 全header先 → 全date後「許可年月日, 有効期限, 許可番号, 日付, 日付」
    if not out["issue_date"] or not out["expiry_date"]:
        # 複数行 merge: 「令和X年」+「Y月Z日」→ 単一行に結合
        merged_lines = []
        i = 0
        while i < len(lines):
            era_m = RE_ERA_YEAR_ONLY.match(lines[i])
            if era_m and i + 1 < len(lines):
                md_m = RE_MONTH_DAY_ONLY.match(lines[i + 1])
                if md_m:
                    merged_lines.append(
                        f"{era_m.group('era')} {era_m.group('y')}年 {md_m.group('m')}月{md_m.group('d')}日"
                    )
                    i += 2
                    continue
            merged_lines.append(lines[i])
            i += 1

        # ヘッダ位置(merged基準)
        issue_idx = expiry_idx = -1
        for i, line in enumerate(merged_lines):
            if RE_ISSUE_HINT.search(line) and issue_idx < 0:
                issue_idx = i
            if RE_EXPIRY_HINT.search(line) and expiry_idx < 0:
                expiry_idx = i

        def is_noise(line: str) -> bool:
            return any(kw in line for kw in ["更新申請", "書類提出期限", "行政庁", "付けで申請", "申請のありました", "ことを証明", "日付:", "付け"])

        def date_era_match(line: str):
            """元号明示の完全日付のみ返す（noise除外）"""
            if is_noise(line):
                return None
            m = RE_JP_DATE.search(line)
            if m and m.group("era"):
                return m
            return None

        # サブパターン2a: issue_header〜expiry_header間に日付がある場合のみ採用
        issue_found_2a = False
        expiry_found_2a = False
        has_date_between = False
        if issue_idx >= 0 and expiry_idx >= 0 and issue_idx < expiry_idx:
            for j in range(issue_idx + 1, expiry_idx):
                if date_era_match(merged_lines[j]):
                    has_date_between = True
                    break
        if has_date_between:
            for j in range(issue_idx + 1, expiry_idx):
                m = date_era_match(merged_lines[j])
                if m and not out["issue_date"]:
                    out["issue_date"] = jp_to_iso(m.group("y"), m.group("m"), m.group("d"), m.group("era"))
                    issue_found_2a = True
                    break
            for j in range(expiry_idx + 1, len(merged_lines)):
                m = date_era_match(merged_lines[j])
                if m and not out["expiry_date"]:
                    out["expiry_date"] = jp_to_iso(m.group("y"), m.group("m"), m.group("d"), m.group("era"))
                    expiry_found_2a = True
                    break

        # サブパターン2b: header連続後に日付列が並ぶ形式（谷野宮組等）
        # expiry_header 以降の最初の2個の日付を issue/expiry に割当
        if not (issue_found_2a and expiry_found_2a):
            anchor = expiry_idx if expiry_idx >= 0 else issue_idx
            if anchor >= 0:
                body_dates = []
                for j in range(anchor + 1, len(merged_lines)):
                    m = date_era_match(merged_lines[j])
                    if m:
                        body_dates.append(m)
                if len(body_dates) >= 1 and not out["issue_date"]:
                    m = body_dates[0]
                    out["issue_date"] = jp_to_iso(m.group("y"), m.group("m"), m.group("d"), m.group("era"))
                if len(body_dates) >= 2 and not out["expiry_date"]:
                    m = body_dates[1]
                    out["expiry_date"] = jp_to_iso(m.group("y"), m.group("m"), m.group("d"), m.group("era"))

    # パターン3: 証明書の場合は expiry_date=許可日+5年（建設業許可は5年有効）
    if is_cert and out["issue_date"] and not out["expiry_date"]:
        from datetime import timedelta
        try:
            issue_dt = date.fromisoformat(out["issue_date"])
            expiry_dt = date(issue_dt.year + 5, issue_dt.month, issue_dt.day) - timedelta(days=1)
            out["expiry_date"] = expiry_dt.isoformat()
            out["error_reason"] = "証明書フォーマット: 有効期限=許可日+5年で自動計算"
        except Exception:
            pass

    # 業種抽出
    found_trades = []
    for trade in TRADES:
        if trade in full_text:
            # 両系対応（「とび・土工工事業」と「とび・土工・コンクリート工事業」の重複判定）
            found_trades.append(trade)
    # 「とび・土工・コンクリート」があったら短い方の「とび・土工」は含めない
    if "とび・土工・コンクリート工事業" in found_trades and "とび・土工工事業" in found_trades:
        found_trades.remove("とび・土工工事業")
    out["trade_categories"] = "|".join(sorted(set(found_trades)))

    # 会社名：許可番号行の前後で「㈱/(株)/㈲/(有)/株式会社/有限会社」を含む行を探す
    for line in lines:
        if any(mark in line for mark in ["株式会社", "有限会社", "(株)", "㈱", "(有)", "㈲", "合同会社"]):
            # 「代表取締役」を含む行は除外（人名が混ざる）
            if "代表" in line or "取締役" in line or "様" in line:
                continue
            out["company_name_raw"] = line.strip()
            break

    # parse_status 判定
    if out["permit_number_full"] and out["expiry_date"]:
        out["parse_status"] = "OK"
    elif out["permit_number_full"] or out["expiry_date"]:
        out["parse_status"] = "REVIEW_NEEDED"
        out["error_reason"] = "一部フィールド欠落"
    else:
        out["parse_status"] = "ERROR"
        out["error_reason"] = "許可番号・有効期限共に抽出不可"

    return out


def run_layout_ocr(pdf_path: Path) -> str:
    from azure.ai.documentintelligence import DocumentIntelligenceClient
    from azure.ai.documentintelligence.models import AnalyzeDocumentRequest
    from azure.core.credentials import AzureKeyCredential

    endpoint = os.environ["AZURE_DI_ENDPOINT"]
    key = os.environ["AZURE_DI_KEY"]
    client = DocumentIntelligenceClient(endpoint=endpoint, credential=AzureKeyCredential(key))

    with pdf_path.open("rb") as f:
        poller = client.begin_analyze_document(
            "prebuilt-layout", AnalyzeDocumentRequest(bytes_source=f.read())
        )
    result = poller.result()

    lines = []
    for page in result.pages or []:
        for line in page.lines or []:
            lines.append(line.content)
    return "\n".join(lines)


def collect_permit_pdfs() -> list[tuple[str, Path]]:
    """(company_id, pdf_path) のリストを返す"""
    out = []
    for cdir in sorted(STAGING.iterdir()):
        if not cdir.is_dir():
            continue
        cid = cdir.name.split("_", 1)[0]
        for f in sorted(cdir.iterdir()):
            if f.is_file() and f.suffix.lower() == ".pdf":
                if any(k in f.name for k in PERMIT_KEYWORDS):
                    out.append((cid, f))
    return out


def main():
    pdfs = collect_permit_pdfs()
    print(f"対象: {len(pdfs)}件", file=sys.stderr)

    records = []
    for i, (cid, pdf) in enumerate(pdfs, 1):
        print(f"[{i}/{len(pdfs)}] {cid}  {pdf.name}", file=sys.stderr)
        try:
            text = run_layout_ocr(pdf)
            fields = extract_fields(text)
        except Exception as exc:
            fields = {k: "" for k in [
                "company_name_raw", "permit_authority_name", "permit_category",
                "permit_year", "permit_number_full", "contractor_number",
                "issue_date", "expiry_date", "trade_categories",
            ]}
            fields["parse_status"] = "ERROR"
            fields["error_reason"] = f"OCR例外: {exc}"
            text = ""

        fields["source_file"] = pdf.name
        fields["company_id"] = cid
        fields["ocr_text"] = text[:2000]  # 先頭2000文字をログ用に残す
        fields["processed_at"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        records.append(fields)
        print(f"    → parse_status={fields['parse_status']}", file=sys.stderr)

    # CSV 出力
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = OUTPUT / f"staging_permits_azure_{ts}.csv"
    fieldnames = [
        "source_file", "company_id", "company_name_raw",
        "permit_authority_name", "permit_category", "permit_year",
        "contractor_number", "permit_number_full",
        "issue_date", "expiry_date", "trade_categories",
        "parse_status", "error_reason", "processed_at", "ocr_text",
    ]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in records:
            writer.writerow(r)
    print(f"\nCSV出力: {csv_path}", file=sys.stderr)
    print(f"完了: {len(records)}件", file=sys.stderr)


if __name__ == "__main__":
    main()
