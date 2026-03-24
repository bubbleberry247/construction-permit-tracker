"""
import_company_master.py
継続取引業者リスト.xlsx を Google Sheets の Companies シートにインポートする。

カラムマッピング (xlsx → Companies スキーマ):
  会社名                   → company_name_raw / company_name_normalized
  代表者名                 → representative_name
  担当者名                 → contact_person
  連絡先（メール・電話）    → contact_email (メール・電話混在のため一括格納)
  ※ 代表敬称・CC・phone専用列は xlsx に存在しないため空文字

Google Sheets ID が空の場合は CSV 出力モードに自動切替。

Usage:
    python src/import_company_master.py
    python src/import_company_master.py --xlsx "C:/path/to/継続取引業者リスト.xlsx"
    python src/import_company_master.py --dry-run
"""

import argparse
import csv
import json
import logging
import re
import sys
import unicodedata
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_PATH = PROJECT_ROOT / "config.json"
OUTPUT_DIR = PROJECT_ROOT / "output"
DEFAULT_XLSX = Path(
    r"C:\Users\owner\KeyenceRK\ロボット作成資料\建築許可証管理システム\元データ\継続取引業者リスト.xlsx"
)

COMPANIES_SHEET = "Companies"
COMPANIES_HEADER = [
    "company_id",
    "company_name_raw",
    "company_name_normalized",
    "representative_name",
    "contact_person",
    "contact_email",
    "contact_email_cc",
    "phone",
    "status",
    "created_at",
    "updated_at",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Normalize
# ---------------------------------------------------------------------------

def normalize_company_name(name: str) -> str:
    """
    会社名の表記ゆれを正規化する。
    - 全角→半角変換（英数字のみ）
    - 全角スペース→半角スペース
    - ㈱ → 株式会社、㈲ → 有限会社
    - （株） → 株式会社（前後スペース含め除去）
    - 末尾の空白除去
    """
    if not name:
        return ""

    # 全角英数字→半角（NFKC正規化）
    normalized = unicodedata.normalize("NFKC", name)

    # 全角スペース→半角スペース（NFKC後に残る場合の保険）
    normalized = normalized.replace("\u3000", " ")

    # 略称→正式表記
    normalized = normalized.replace("㈱", "株式会社")
    normalized = normalized.replace("㈲", "有限会社")

    # （株）→ 株式会社（前後スペースごと除去してから置換）
    normalized = re.sub(r"\s*[（(]株[）)]\s*", "株式会社", normalized)
    normalized = re.sub(r"\s*[（(]有[）)]\s*", "有限会社", normalized)

    return normalized.strip()


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config(config_path: Path) -> dict[str, Any]:
    """config.json を読み込む。"""
    if not config_path.exists():
        raise FileNotFoundError(f"config.json が見つかりません: {config_path}")
    with config_path.open(encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# xlsx 読み込み
# ---------------------------------------------------------------------------

def load_xlsx(xlsx_path: Path) -> list[dict[str, Any]]:
    """
    xlsx を読み込み、Companies スキーマの dict リストを返す。
    行ごとのエラーはログに記録してスキップする。
    """
    try:
        import openpyxl  # noqa: PLC0415
    except ImportError:
        logger.error("openpyxl がインストールされていません: pip install openpyxl")
        sys.exit(1)

    if not xlsx_path.exists():
        raise FileNotFoundError(
            f"xlsx ファイルが見つかりません: {xlsx_path}\n"
            "  --xlsx オプションで正しいパスを指定してください。"
        )

    wb = openpyxl.load_workbook(xlsx_path)
    ws = wb.active

    rows = list(ws.iter_rows(min_row=1, values_only=True))
    if not rows:
        raise ValueError("xlsx にデータがありません。")

    # ヘッダ行（1行目）からカラムインデックスを取得
    header = [str(c).strip() if c is not None else "" for c in rows[0]]
    logger.info("xlsx ヘッダ: %s", header)

    def col_idx(name: str) -> int | None:
        """ヘッダ名が部分一致する最初のインデックスを返す。"""
        for i, h in enumerate(header):
            if name in h:
                return i
        return None

    idx_company = col_idx("会社名")
    idx_rep = col_idx("代表者名")
    idx_contact = col_idx("担当者名")
    idx_email = col_idx("連絡先")

    if idx_company is None:
        raise ValueError("xlsx に '会社名' 列が見つかりません。ヘッダを確認してください。")

    now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    records: list[dict[str, Any]] = []

    for row_num, row in enumerate(rows[1:], start=2):
        try:
            company_raw = str(row[idx_company]).strip() if row[idx_company] else ""
            if not company_raw:
                # 会社名が空の行はスキップ
                continue

            representative = (
                str(row[idx_rep]).strip()
                if idx_rep is not None and row[idx_rep]
                else ""
            )
            contact_person = (
                str(row[idx_contact]).strip()
                if idx_contact is not None and row[idx_contact]
                else ""
            )
            contact_email = (
                str(row[idx_email]).strip()
                if idx_email is not None and row[idx_email]
                else ""
            )

            records.append(
                {
                    "company_id": str(uuid.uuid4()),
                    "company_name_raw": company_raw,
                    "company_name_normalized": normalize_company_name(company_raw),
                    "representative_name": representative,
                    "contact_person": contact_person,
                    "contact_email": contact_email,
                    "contact_email_cc": "",
                    "phone": "",
                    "status": "ACTIVE",
                    "created_at": now_str,
                    "updated_at": now_str,
                }
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("行 %d 読み込みエラー（スキップ）: %s", row_num, exc)

    logger.info("xlsx から %d 件読み込み完了", len(records))
    return records


# ---------------------------------------------------------------------------
# Google Sheets
# ---------------------------------------------------------------------------

def get_existing_normalized_names(service: Any, spreadsheet_id: str) -> set[str]:
    """Companies シートから既存の company_name_normalized 一覧を取得する。"""
    try:
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=f"{COMPANIES_SHEET}!A1:K")
            .execute()
        )
    except Exception:  # noqa: BLE001
        return set()

    values = result.get("values", [])
    if len(values) < 2:
        return set()

    header = values[0]
    try:
        norm_idx = header.index("company_name_normalized")
    except ValueError:
        return set()

    return {
        row[norm_idx].strip()
        for row in values[1:]
        if len(row) > norm_idx and row[norm_idx]
    }


def ensure_sheet_header(service: Any, spreadsheet_id: str) -> None:
    """Companies シートが空なら COMPANIES_HEADER を書き込む。"""
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=f"{COMPANIES_SHEET}!A1:A1")
        .execute()
    )
    if not result.get("values"):
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{COMPANIES_SHEET}!A1",
            valueInputOption="RAW",
            body={"values": [COMPANIES_HEADER]},
        ).execute()
        logger.info("Companies シートにヘッダを書き込みました。")


def append_to_sheets(
    service: Any, spreadsheet_id: str, records: list[dict[str, Any]]
) -> None:
    """Companies シートにレコードを追記する。"""
    rows = [[rec[col] for col in COMPANIES_HEADER] for rec in records]
    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"{COMPANIES_SHEET}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()


def build_sheets_service(credentials_file: str) -> Any:
    """サービスアカウント JSON から Sheets API クライアントを生成する。"""
    try:
        from google.oauth2 import service_account  # noqa: PLC0415
        from googleapiclient.discovery import build  # noqa: PLC0415
    except ImportError:
        logger.error(
            "google-api-python-client が未インストールです: "
            "pip install google-api-python-client google-auth"
        )
        sys.exit(1)

    creds_path = Path(credentials_file)
    if not creds_path.exists():
        raise FileNotFoundError(
            f"認証情報ファイルが見つかりません: {creds_path}\n"
            "  config.json の GOOGLE_SERVICE_ACCOUNT_FILE を確認してください。"
        )

    creds = service_account.Credentials.from_service_account_file(
        str(creds_path),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds)


# ---------------------------------------------------------------------------
# CSV フォールバック
# ---------------------------------------------------------------------------

def export_csv(records: list[dict[str, Any]]) -> Path:
    """output/companies_import_YYYYMMDD.csv に書き出す。"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    csv_path = OUTPUT_DIR / f"companies_import_{date_str}.csv"

    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=COMPANIES_HEADER)
        writer.writeheader()
        writer.writerows(records)

    return csv_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="継続取引業者リスト.xlsx を Google Sheets Companies シートにインポートする"
    )
    parser.add_argument(
        "--xlsx",
        default=str(DEFAULT_XLSX),
        help="xlsx ファイルのパス（省略時はデフォルトパスを使用）",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="書き込みを行わず内容をコンソールに表示する",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # 1. config.json 読み込み
    config = load_config(CONFIG_PATH)
    sheets_id: str = config.get("GOOGLE_SHEETS_ID", "")
    credentials_file: str = config.get("GOOGLE_SERVICE_ACCOUNT_FILE", "")
    csv_mode = not sheets_id

    if csv_mode:
        logger.info("GOOGLE_SHEETS_ID が未設定のため CSV 出力モードで動作します。")

    # 2. xlsx 読み込み
    xlsx_path = Path(args.xlsx)
    records = load_xlsx(xlsx_path)

    if not records:
        logger.warning("インポート対象レコードが 0 件です。終了します。")
        return

    # 3. dry-run
    if args.dry_run:
        logger.info("--- DRY RUN: 以下の %d 件を書き込む予定 ---", len(records))
        for rec in records:
            print(
                f"  [{rec['company_name_normalized']}]"
                f"  担当: {rec['contact_person']}"
                f"  連絡先: {rec['contact_email']}"
            )
        logger.info("DRY RUN 完了。実際の書き込みは行っていません。")
        return

    new_count = 0
    skip_count = 0
    error_count = 0

    # 4 & 5. Google Sheets upsert or CSV 出力
    if csv_mode:
        try:
            csv_path = export_csv(records)
            new_count = len(records)
            logger.info("CSV 出力完了: file:///%s", str(csv_path).replace("\\", "/"))
        except Exception as exc:  # noqa: BLE001
            logger.error("CSV 出力エラー: %s", exc)
            error_count = len(records)
    else:
        try:
            service = build_sheets_service(credentials_file)
            ensure_sheet_header(service, sheets_id)
            existing = get_existing_normalized_names(service, sheets_id)
            logger.info("既存レコード（正規化名）: %d 件", len(existing))

            to_insert: list[dict[str, Any]] = []
            for rec in records:
                if rec["company_name_normalized"] in existing:
                    logger.info("SKIP（重複）: %s", rec["company_name_normalized"])
                    skip_count += 1
                else:
                    to_insert.append(rec)

            if to_insert:
                append_to_sheets(service, sheets_id, to_insert)
                new_count = len(to_insert)
                logger.info("Sheets に %d 件追記しました。", new_count)
            else:
                logger.info("追記対象なし。")

            # CSV も常に出力（ocr_permit.py がローカル CSV を参照するため）
            try:
                csv_path = export_csv(records)
                logger.info(
                    "CSV 同時出力完了: file:///%s",
                    str(csv_path).replace("\\", "/"),
                )
            except Exception as csv_exc:  # noqa: BLE001
                logger.warning("CSV 同時出力に失敗しました: %s", csv_exc)

        except FileNotFoundError as exc:
            logger.error("%s", exc)
            sys.exit(1)
        except Exception as exc:  # noqa: BLE001
            logger.error("Sheets API エラー: %s", exc)
            error_count = len(records)

    # 6. サマリー表示
    print(f"\n=== インポート結果サマリー ===")
    print(f"  新規: {new_count} 件")
    print(f"  スキップ: {skip_count} 件")
    print(f"  エラー: {error_count} 件")


if __name__ == "__main__":
    main()
