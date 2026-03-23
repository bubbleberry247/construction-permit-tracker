"""
register_sheets.py — ステージングCSVをGoogle Sheets Permitsタブへ Upsert する。

upsert キー: company_id + permit_authority_name_normalized + contractor_number + permit_category
BLOCKED:      parse_status != "OK"  |  company_id が空/nan (TIER3_UNKNOWN_COMPANY)
dry-run:      GOOGLE_SHEETS_ID が空、または --dry-run フラグ

Usage:
    python src/register_sheets.py                          # output/ 最新 staging_*.csv
    python src/register_sheets.py output/staging_20260324.csv
    python src/register_sheets.py --dry-run
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_PATH = PROJECT_ROOT / "config.json"
PERMITS_SHEET = "Permits"
CHECKLIST_SHEET = "DocumentChecklist"

PERMITS_HEADERS: list[str] = [
    "permit_id", "company_id", "company_name_raw",
    "permit_authority_name", "permit_authority_name_normalized", "permit_authority_type", "permit_category",
    "permit_year", "contractor_number", "permit_number_full",
    "trade_categories", "issue_date", "expiry_date", "renewal_deadline_date",
    "current_status", "evidence_renewal_application", "renewal_application_date",
    "mlit_confirmed_date", "mlit_confirm_result", "mlit_screenshot_url",
    "permit_file_path", "permit_file_share_url", "permit_file_version", "evidence_file_path",
    "last_received_date", "source_file", "source_file_hash",
    "parse_status", "error_category", "error_reason",
    "note", "created_at", "updated_at",
]

DOCUMENT_CHECKLIST_HEADERS: list[str] = [
    "check_id", "company_id", "submission_date",
    "新規継続取引申請書", "建設業許可証", "決算書前年度", "決算書前々年度",
    "会社案内", "工事経歴書", "取引先一覧表", "労働安全衛生誓約書",
    "資格略字一覧", "労働者名簿一覧表", "個人事業主_青色申告書",
    "source_pdf_url", "llm_classification_raw", "created_at", "updated_at",
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)


def load_config(config_path: Path = CONFIG_PATH) -> dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(f"config.json が見つかりません: {config_path}")
    with config_path.open(encoding="utf-8") as f:
        return json.load(f)


def get_sheets_client(credentials_file: str) -> Any | None:
    """サービスアカウント JSON から gspread クライアントを返す。失敗時は None。"""
    creds_path = Path(credentials_file)
    if not creds_path.exists():
        logger.warning("認証情報ファイルが見つかりません: %s → dry-run で動作します", creds_path)
        return None
    try:
        import gspread  # noqa: PLC0415
        from google.oauth2.service_account import Credentials  # noqa: PLC0415
    except ImportError:
        logger.error("gspread / google-auth が未インストール: pip install gspread google-auth")
        return None
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(str(creds_path), scopes=scopes)
    return gspread.authorize(creds)


def call_with_retry(fn: Any, max_retries: int = 3, base_delay: float = 1.0) -> Any:
    """Exponential Backoff リトライ。401/403 は TIER2_AUTH_ERROR として即中断。"""
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            if any(c in str(exc) for c in ("401", "403", "UNAUTHENTICATED", "PERMISSION_DENIED")):
                logger.error("TIER2_AUTH_ERROR: リトライしません: %s", exc)
                raise
            if attempt >= max_retries:
                last_exc = exc
                break
            delay = base_delay * (2 ** attempt)
            logger.warning("TIER1_SHEETS_TIMEOUT: リトライ %d/%d (%.1fs): %s", attempt + 1, max_retries, delay, exc)
            time.sleep(delay)
    raise RuntimeError(f"Sheets API が {max_retries} 回リトライ後も失敗") from last_exc


def _is_blank(value: Any) -> bool:
    return value is None or str(value).strip().lower() in ("", "nan", "none", "null")


def _to_date(value: Any) -> date | None:
    if _is_blank(value):
        return None
    try:
        from dateutil import parser as dp  # noqa: PLC0415
        return dp.parse(str(value)).date()
    except Exception:  # noqa: BLE001
        return None


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def read_staging_csv(csv_path: Path) -> list[dict[str, Any]]:
    """parse_status=OK かつ company_id が非空の行のみ返す。それ以外はログしてスキップ。"""
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV が見つかりません: {csv_path}")
    valid: list[dict[str, Any]] = []
    total = 0
    with csv_path.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            total += 1
            src = row.get("source_file", f"行{total}")
            if str(row.get("parse_status", "")).strip() != "OK":
                logger.info("SKIP (parse_status=%s): %s", row.get("parse_status"), src)
                continue
            if _is_blank(row.get("company_id")):
                logger.warning("BLOCKED: TIER3_UNKNOWN_COMPANY - company_id not set: %s", src)
                continue
            valid.append(dict(row))
    logger.info("CSV読み込み: 合計 %d 行 → 有効 %d 行", total, len(valid))
    return valid


def find_latest_staging_csv(staging_dir: Path) -> Path:
    csvs = sorted(staging_dir.glob("staging_*.csv"), reverse=True)
    if not csvs:
        raise FileNotFoundError(f"output/ に staging_*.csv が見つかりません: {staging_dir}")
    return csvs[0]


def determine_current_status(row: dict[str, Any]) -> str:
    """Models.gs の GAS ロジックと同等のステータス算出。"""
    today = date.today()
    expiry = _to_date(row.get("expiry_date"))
    deadline = _to_date(row.get("renewal_deadline_date"))
    renew_app = _to_date(row.get("renewal_application_date"))
    evidence_ok = str(row.get("evidence_renewal_application", "")).lower() in ("true", "1", "yes")
    evidence_file = row.get("evidence_file_path", "")

    if _is_blank(expiry) or _is_blank(row.get("contractor_number")) or _is_blank(row.get("trade_categories")):
        return "DEFICIENT"
    if _is_blank(row.get("company_id")):
        return "REQUIRES_ACTION"

    d_expiry = (expiry - today).days if expiry else None
    d_deadline = (deadline - today).days if deadline else None

    if d_expiry is not None and d_expiry < 0 and evidence_ok and not _is_blank(evidence_file) \
            and renew_app is not None and expiry is not None and renew_app <= expiry:
        return "RENEWAL_IN_PROGRESS"
    if d_expiry is not None and d_expiry < 0:
        return "EXPIRED"
    if d_deadline is not None and d_deadline < 0:
        return "RENEWAL_OVERDUE"
    if d_expiry is not None and d_expiry <= 90:
        return "EXPIRING"
    return "VALID"


def _get_sheet(spreadsheet: Any, name: str) -> Any:
    try:
        return spreadsheet.worksheet(name)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"シート '{name}' が見つかりません: {exc}") from exc


def _ensure_header(sheet: Any, headers: list[str], max_retries: int, base_delay: float) -> None:
    row1 = call_with_retry(lambda: sheet.row_values(1), max_retries, base_delay)
    if row1 != headers:
        call_with_retry(lambda: sheet.insert_row(headers, index=1), max_retries, base_delay)
        logger.info("ヘッダ行を書き込みました: %s", sheet.title)


def find_existing_permit(
    sheet: Any, company_id: str, permit_auth_normalized: str,
    contractor_number: str, permit_category: str,
    max_retries: int = 3, base_delay: float = 1.0,
) -> int | None:
    """4キー一致行の 1-based 行番号を返す。見つからなければ None。"""
    all_values: list[list[str]] = call_with_retry(lambda: sheet.get_all_values(), max_retries, base_delay)
    if len(all_values) < 2:
        return None
    headers = all_values[0]
    try:
        i_cid = headers.index("company_id")
        i_auth = headers.index("permit_authority_name_normalized")
        i_num = headers.index("contractor_number")
        i_cat = headers.index("permit_category")
    except ValueError:
        logger.error("Permits シートに必要なカラムがありません")
        return None
    for row_idx, row in enumerate(all_values[1:], start=2):
        def g(i: int) -> str:
            return row[i].strip() if i < len(row) else ""
        if (g(i_cid) == company_id.strip() and g(i_auth) == permit_auth_normalized.strip()
                and g(i_num) == contractor_number.strip() and g(i_cat) == permit_category.strip()):
            return row_idx
    return None


def upsert_permit(
    sheet: Any, row_data: dict[str, Any], existing_row_idx: int | None,
    max_retries: int = 3, base_delay: float = 1.0, dry_run: bool = False,
) -> str:
    """INSERT or UPDATE。dry_run=True のときは実際の書き込みを行わない。"""
    now = _now_str()
    if existing_row_idx is not None:
        if dry_run:
            logger.info("[DRY-RUN] UPDATE 行%d: %s", existing_row_idx, row_data.get("source_file"))
            return "UPDATE"
        existing = call_with_retry(lambda: sheet.row_values(existing_row_idx), max_retries, base_delay)
        h1 = call_with_retry(lambda: sheet.row_values(1), max_retries, base_delay)
        try:
            ver = int(existing[h1.index("permit_file_version")]) if "permit_file_version" in h1 else 0
        except (ValueError, IndexError):
            ver = 0
        row_data["permit_file_version"] = ver + 1
        row_data["updated_at"] = now
        new_row = [str(row_data.get(h, "")) for h in PERMITS_HEADERS]
        call_with_retry(lambda: sheet.update(f"A{existing_row_idx}", [new_row]), max_retries, base_delay)
        return "UPDATE"
    else:
        if dry_run:
            logger.info("[DRY-RUN] INSERT: %s", row_data.get("source_file"))
            return "INSERT"
        row_data.setdefault("permit_id", str(uuid.uuid4()))
        row_data.setdefault("permit_file_version", 1)
        row_data["created_at"] = now
        row_data["updated_at"] = now
        call_with_retry(lambda: sheet.append_row([str(row_data.get(h, "")) for h in PERMITS_HEADERS]),
                        max_retries, base_delay)
        return "INSERT"


def register_document_checklist(
    checklist_sheet: Any, row_data: dict[str, Any],
    max_retries: int = 3, base_delay: float = 1.0, dry_run: bool = False,
) -> None:
    """document_type フィールドがある行のみ DocumentChecklist を upsert する。"""
    doc_type = str(row_data.get("document_type", "")).strip()
    if _is_blank(doc_type):
        return
    company_id = row_data.get("company_id", "")
    submission_date = row_data.get("last_received_date", "")
    if dry_run:
        logger.info("[DRY-RUN] DocumentChecklist upsert: company_id=%s type=%s", company_id, doc_type)
        return

    all_values: list[list[str]] = call_with_retry(lambda: checklist_sheet.get_all_values(), max_retries, base_delay)
    now = _now_str()
    target_row_idx: int | None = None

    if len(all_values) >= 2:
        headers = all_values[0]
        try:
            cid_i, date_i = headers.index("company_id"), headers.index("submission_date")
        except ValueError:
            logger.warning("DocumentChecklist のヘッダ構造が異なります")
            return
        for row_idx, row in enumerate(all_values[1:], start=2):
            if (row[cid_i].strip() if cid_i < len(row) else "") == str(company_id).strip() \
                    and (row[date_i].strip() if date_i < len(row) else "") == str(submission_date).strip():
                target_row_idx = row_idx
                break

    if target_row_idx is not None:
        headers = all_values[0]
        try:
            doc_i = headers.index(doc_type)
            upd_i = headers.index("updated_at")
        except ValueError:
            logger.warning("DocumentChecklist に列 '%s' が見つかりません", doc_type)
            return
        call_with_retry(lambda: checklist_sheet.update_cell(target_row_idx, doc_i + 1, "TRUE"), max_retries, base_delay)
        call_with_retry(lambda: checklist_sheet.update_cell(target_row_idx, upd_i + 1, now), max_retries, base_delay)
    else:
        new_row_data = {h: "" for h in DOCUMENT_CHECKLIST_HEADERS}
        new_row_data.update({"check_id": str(uuid.uuid4()), "company_id": company_id,
                             "submission_date": submission_date,
                             "source_pdf_url": row_data.get("permit_file_share_url", ""),
                             "created_at": now, "updated_at": now})
        if doc_type in DOCUMENT_CHECKLIST_HEADERS:
            new_row_data[doc_type] = "TRUE"
        call_with_retry(lambda: checklist_sheet.append_row([str(new_row_data.get(h, "")) for h in DOCUMENT_CHECKLIST_HEADERS]),
                        max_retries, base_delay)

    logger.info("DocumentChecklist 更新: company_id=%s type=%s", company_id, doc_type)


def main(csv_path: Path | None = None, dry_run: bool = False) -> None:
    config = load_config()
    sheets_id: str = config.get("GOOGLE_SHEETS_ID", "")
    credentials_file: str = config.get("GOOGLE_CREDENTIALS_FILE", "")
    max_retries: int = int(config.get("RETRY_MAX", 3))
    base_delay: float = float(config.get("RETRY_BASE_DELAY_SEC", 1.0))
    staging_dir = Path(config.get("DATA_ROOT", str(PROJECT_ROOT))) / config.get("STAGING_CSV_DIR", "output")

    if csv_path is None:
        csv_path = find_latest_staging_csv(staging_dir)
    logger.info("処理対象 CSV: %s", csv_path)

    if not sheets_id:
        logger.info("GOOGLE_SHEETS_ID が未設定のため dry-run モードで動作します")
        dry_run = True

    rows = read_staging_csv(csv_path)
    if not rows:
        logger.warning("登録対象行が 0 件です。終了します。")
        _print_summary(0, 0, 0, 0, 0)
        return

    permits_sheet: Any = None
    checklist_sheet: Any = None

    if not dry_run:
        client = get_sheets_client(credentials_file)
        if client is None:
            logger.warning("Sheets クライアント取得失敗 → dry-run に切り替えます")
            dry_run = True
        else:
            try:
                ss = call_with_retry(lambda: client.open_by_key(sheets_id), max_retries, base_delay)
                permits_sheet = _get_sheet(ss, PERMITS_SHEET)
                checklist_sheet = _get_sheet(ss, CHECKLIST_SHEET)
                _ensure_header(permits_sheet, PERMITS_HEADERS, max_retries, base_delay)
            except Exception as exc:  # noqa: BLE001
                logger.error("Sheets 接続エラー: %s", exc)
                sys.exit(1)

    registered = updated = errors = 0

    for row in rows:
        source = row.get("source_file", "?")
        try:
            row["current_status"] = determine_current_status(row)
            cid = str(row.get("company_id", "")).strip()
            auth = str(row.get("permit_authority_name_normalized", "")).strip()
            num = str(row.get("contractor_number", "")).strip()
            cat = str(row.get("permit_category", "")).strip()

            existing_idx = None if dry_run else find_existing_permit(
                permits_sheet, cid, auth, num, cat, max_retries, base_delay)
            action = upsert_permit(permits_sheet, row, existing_idx, max_retries, base_delay, dry_run)

            if action == "INSERT":
                registered += 1
                logger.info("INSERT: %s (company_id=%s)", source, cid)
            else:
                updated += 1
                logger.info("UPDATE: %s (company_id=%s)", source, cid)

            register_document_checklist(
                checklist_sheet if not dry_run else None,
                row, max_retries, base_delay, dry_run,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("エラー (source=%s): %s", source, exc)
            errors += 1

    _print_summary(len(rows), 0, registered, updated, errors)


def _print_summary(total: int, skipped: int, registered: int, updated: int, errors: int) -> None:
    print(f"\n=== 登録結果サマリー ===\n  合計行数:{total}  スキップ:{skipped}  新規:{registered}  更新:{updated}  エラー:{errors}")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ステージング CSV を Google Sheets Permits タブへ Upsert する")
    p.add_argument("csv_path", nargs="?", default=None, help="CSV ファイルパス（省略時は output/ 最新）")
    p.add_argument("--dry-run", action="store_true", help="書き込みを行わず内容をコンソールに表示する")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    main(csv_path=Path(args.csv_path) if args.csv_path else None, dry_run=args.dry_run)
