"""
mlit_confirm.py — MLIT etsuran2 で許可証の現在状態を確認し、スクリーンショットを保存する。

Usage:
    python src/mlit_confirm.py                    # Google Sheets からアクティブ permit を取得して全件確認
    python src/mlit_confirm.py --csv path/to.csv  # 特定の staging CSV から対象を取得
    python src/mlit_confirm.py --dry-run          # URL生成のみ（Playwright非実行）

確認対象ステータス: EXPIRING / RENEWAL_OVERDUE / EXPIRED / RENEWAL_IN_PROGRESS

注意: MLIT 一括取得は規約違反。1件ずつ 3秒ウェイトを入れること。
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_PATH = PROJECT_ROOT / "config.json"

MLIT_SEARCH_URL = "https://etsuran2.mlit.go.jp/TAKKEN/kensetsuKensaku.do"

# 確認対象とするステータス
TARGET_STATUSES = {"EXPIRING", "RENEWAL_OVERDUE", "EXPIRED", "RENEWAL_IN_PROGRESS"}

PERMITS_SHEET = "Permits"

# 許可区分: 知事許可は "2"、大臣許可は "1"
_PERMIT_TYPE_CODE = {
    "知事": "2",
    "大臣": "1",
}

# 都道府県コード（etsuran2 用・知事許可の場合に必要）
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

MLIT_WAIT_SEC = 3  # MLIT 規約対応: 1件ごとのウェイト

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 設定ロード
# ---------------------------------------------------------------------------

def load_config(config_path: Path = CONFIG_PATH) -> dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(f"config.json が見つかりません: {config_path}")
    with config_path.open(encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Google Sheets クライアント（register_sheets.py のパターンを踏襲）
# ---------------------------------------------------------------------------

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
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(str(creds_path), scopes=scopes)
    return gspread.authorize(creds)


def call_with_retry(fn: Any, max_retries: int = 3, base_delay: float = 1.0) -> Any:
    """Exponential Backoff リトライ。401/403 は即中断。"""
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            if any(c in str(exc) for c in ("401", "403", "UNAUTHENTICATED", "PERMISSION_DENIED")):
                logger.error("AUTH_ERROR: リトライしません: %s", exc)
                raise
            if attempt >= max_retries:
                last_exc = exc
                break
            delay = base_delay * (2 ** attempt)
            logger.warning("SHEETS_TIMEOUT: リトライ %d/%d (%.1fs): %s", attempt + 1, max_retries, delay, exc)
            time.sleep(delay)
    raise RuntimeError(f"Sheets API が {max_retries} 回リトライ後も失敗") from last_exc


# ---------------------------------------------------------------------------
# 許可証データ取得
# ---------------------------------------------------------------------------

def fetch_permits_from_sheets(
    client: Any,
    sheets_id: str,
    max_retries: int = 3,
    base_delay: float = 1.0,
) -> list[dict[str, Any]]:
    """Google Sheets Permits シートから確認対象 permit を取得する。"""
    ss = call_with_retry(lambda: client.open_by_key(sheets_id), max_retries, base_delay)
    sheet = ss.worksheet(PERMITS_SHEET)
    all_values: list[list[str]] = call_with_retry(lambda: sheet.get_all_values(), max_retries, base_delay)

    if len(all_values) < 2:
        logger.info("Permits シートにデータが存在しません")
        return []

    headers = all_values[0]
    permits: list[dict[str, Any]] = []
    for row in all_values[1:]:
        record = {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}
        status = record.get("current_status", "").strip()
        if status in TARGET_STATUSES:
            permits.append(record)

    logger.info(
        "Sheets から取得: 合計 %d 行 → 確認対象 %d 件",
        len(all_values) - 1,
        len(permits),
    )
    return permits


def fetch_permits_from_csv(csv_path: Path) -> list[dict[str, Any]]:
    """staging CSV から確認対象 permit を取得する（current_status でフィルタリング）。"""
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV が見つかりません: {csv_path}")
    permits: list[dict[str, Any]] = []
    total = 0
    with csv_path.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            total += 1
            status = row.get("current_status", "").strip()
            if status in TARGET_STATUSES:
                permits.append(dict(row))
    logger.info("CSV から取得: 合計 %d 行 → 確認対象 %d 件", total, len(permits))
    return permits


# ---------------------------------------------------------------------------
# URL 生成・許可区分判定
# ---------------------------------------------------------------------------

def get_permit_type_code(authority_normalized: str) -> str:
    """permit_authority_name_normalized から知事/大臣区分コードを返す。"""
    if "大臣" in authority_normalized:
        return "1"
    return "2"  # デフォルト: 知事許可


def get_pref_code(authority_normalized: str) -> str | None:
    """都道府県コードを返す。大臣許可・不明の場合は None。"""
    return _PREF_CODE.get(authority_normalized)


def build_search_url(authority_normalized: str, contractor_number: str) -> str:
    """
    MLIT etsuran2 の検索 URL を生成する。
    GET パラメータはフォーム POST で使用するため、URL はエントリポイントのみ返す。
    """
    return MLIT_SEARCH_URL


# ---------------------------------------------------------------------------
# スクリーンショット保存先
# ---------------------------------------------------------------------------

def get_screenshot_path(data_root: Path, contractor_number: str) -> Path:
    """スクリーンショット保存パスを返す。"""
    date_str = datetime.now().strftime("%Y%m%d")
    safe_number = contractor_number.replace("/", "_").replace("\\", "_")
    screenshots_dir = data_root / "data" / "mlit_screenshots"
    screenshots_dir.mkdir(parents=True, exist_ok=True)
    return screenshots_dir / f"{date_str}_{safe_number}.png"


# ---------------------------------------------------------------------------
# Playwright による MLIT etsuran2 操作
# ---------------------------------------------------------------------------

def confirm_permit_with_playwright(
    permit: dict[str, Any],
    screenshot_path: Path,
    timeout_ms: int = 15000,
) -> str:
    """
    Playwright で MLIT etsuran2 を操作し、確認結果を返す。
    戻り値: "一致" | "不一致" | "確認不可"
    """
    try:
        from playwright.sync_api import sync_playwright  # noqa: PLC0415
    except ImportError:
        logger.error("playwright が未インストール: pip install playwright && playwright install chromium")
        return "確認不可"

    authority_normalized = permit.get("permit_authority_name_normalized", "").strip()
    contractor_number = permit.get("contractor_number", "").strip()
    permit_type_code = get_permit_type_code(authority_normalized)
    pref_code = get_pref_code(authority_normalized)

    if not contractor_number:
        logger.warning("contractor_number が空のためスキップ: %s", permit.get("permit_id"))
        return "確認不可"

    result = "確認不可"
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.set_default_timeout(timeout_ms)

            try:
                page.goto(MLIT_SEARCH_URL)

                # 許可区分（知事/大臣）を選択
                # select name の候補: "daijin" 等（実際のフォーム構造に依存）
                try:
                    page.select_option("select[name='daijin']", permit_type_code)
                except Exception:
                    logger.debug("許可区分 select が見つかりません（無視）")

                # 都道府県選択（知事許可の場合）
                if permit_type_code == "2" and pref_code:
                    try:
                        page.select_option("select[name='todofuken']", pref_code)
                    except Exception:
                        logger.debug("都道府県 select が見つかりません（無視）")

                # 業者番号入力
                try:
                    page.fill("input[name='gyosyaNo']", contractor_number)
                except Exception:
                    # フォールバック: type="text" の最初の input
                    try:
                        page.fill("input[type='text']:first-of-type", contractor_number)
                    except Exception:
                        logger.warning("業者番号入力フィールドが見つかりません: %s", contractor_number)
                        screenshot_path.parent.mkdir(parents=True, exist_ok=True)
                        page.screenshot(path=str(screenshot_path))
                        browser.close()
                        return "確認不可"

                # フォーム送信
                try:
                    page.click("input[type='submit']")
                except Exception:
                    try:
                        page.click("button[type='submit']")
                    except Exception:
                        page.keyboard.press("Enter")

                # 結果ページ待機
                page.wait_for_load_state("networkidle", timeout=timeout_ms)

                # スクリーンショット保存
                screenshot_path.parent.mkdir(parents=True, exist_ok=True)
                page.screenshot(path=str(screenshot_path), full_page=True)

                # 結果判定
                content = page.content()
                if "該当する建設業者はありません" in content:
                    result = "不一致"
                elif contractor_number in content:
                    result = "一致"
                else:
                    # 許可番号全体での再確認
                    permit_number_full = permit.get("permit_number_full", "").strip()
                    if permit_number_full and permit_number_full in content:
                        result = "一致"
                    else:
                        result = "確認不可"

            except Exception as inner_exc:
                logger.warning(
                    "MLIT 操作エラー (contractor_number=%s): %s",
                    contractor_number,
                    inner_exc,
                )
                try:
                    screenshot_path.parent.mkdir(parents=True, exist_ok=True)
                    page.screenshot(path=str(screenshot_path))
                except Exception:
                    pass
                result = "確認不可"
            finally:
                browser.close()

    except Exception as exc:
        logger.error("Playwright 起動エラー: %s", exc)
        result = "確認不可"

    return result


# ---------------------------------------------------------------------------
# Google Sheets への結果書き戻し
# ---------------------------------------------------------------------------

def _a1(row: int, col: int) -> str:
    """1-based (row, col) を A1 記法に変換する（例: (2, 18) → 'R2'）。"""
    result = ""
    c = col
    while c > 0:
        c, rem = divmod(c - 1, 26)
        result = chr(65 + rem) + result
    return f"{result}{row}"


def update_permit_mlit_result(
    client: Any,
    sheets_id: str,
    permit_id: str,
    confirmed_date: str,
    confirm_result: str,
    screenshot_path: str,
    max_retries: int = 3,
    base_delay: float = 1.0,
) -> bool:
    """Permits シートの mlit_confirmed_date と mlit_confirm_result を更新する。"""
    try:
        ss = call_with_retry(lambda: client.open_by_key(sheets_id), max_retries, base_delay)
        sheet = ss.worksheet(PERMITS_SHEET)
        all_values: list[list[str]] = call_with_retry(lambda: sheet.get_all_values(), max_retries, base_delay)

        if len(all_values) < 2:
            return False

        headers = all_values[0]
        try:
            pid_i = headers.index("permit_id")
            date_i = headers.index("mlit_confirmed_date")
            result_i = headers.index("mlit_confirm_result")
            screenshot_i = headers.index("mlit_screenshot_url")
        except ValueError as e:
            logger.error("Permits シートに必要なカラムがありません: %s", e)
            return False

        for row_idx, row in enumerate(all_values[1:], start=2):
            cell_val = row[pid_i].strip() if pid_i < len(row) else ""
            if cell_val == permit_id.strip():
                # 3フィールドを1回の batch_update で更新（整合性確保 + API呼び出し削減）
                # gspread は A1 記法のみサポート（R1C1 は INVALID_ARGUMENT になる）
                updates = [
                    {"range": _a1(row_idx, date_i + 1),       "values": [[confirmed_date]]},
                    {"range": _a1(row_idx, result_i + 1),     "values": [[confirm_result]]},
                    {"range": _a1(row_idx, screenshot_i + 1), "values": [[screenshot_path]]},
                ]
                call_with_retry(
                    lambda u=updates: sheet.batch_update(u),
                    max_retries,
                    base_delay,
                )
                return True

        logger.warning("permit_id が Sheets で見つかりません: %s", permit_id)
        return False

    except Exception as exc:
        logger.error("Sheets 更新エラー (permit_id=%s): %s", permit_id, exc)
        return False


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def main(csv_path: Path | None, dry_run: bool) -> None:
    config = load_config()
    data_root = Path(config.get("DATA_ROOT", str(PROJECT_ROOT)))
    sheets_id: str = config.get("GOOGLE_SHEETS_ID", "")
    credentials_file: str = config.get("GOOGLE_CREDENTIALS_FILE", "")
    max_retries: int = int(config.get("RETRY_MAX", 3))
    base_delay: float = float(config.get("RETRY_BASE_DELAY_SEC", 1.0))

    # 確認対象の取得
    permits: list[dict[str, Any]] = []

    if csv_path is not None:
        permits = fetch_permits_from_csv(csv_path)
    elif sheets_id:
        client = get_sheets_client(credentials_file)
        if client is None:
            logger.warning("Sheets クライアント取得失敗 → dry-run に切り替えます")
            dry_run = True
        else:
            permits = fetch_permits_from_sheets(client, sheets_id, max_retries, base_delay)
    else:
        logger.warning("GOOGLE_SHEETS_ID が未設定かつ --csv も未指定 → dry-run モードで動作します")
        dry_run = True

    if not permits:
        logger.info("確認対象の permit が 0 件です。終了します。")
        return

    logger.info("確認対象: %d 件 (dry_run=%s)", len(permits), dry_run)

    # Sheets クライアント（書き戻し用）
    sheets_client: Any = None
    if not dry_run and sheets_id and csv_path is None:
        sheets_client = get_sheets_client(credentials_file)

    success = 0
    failed = 0
    today_str = datetime.now().strftime("%Y-%m-%d")

    for i, permit in enumerate(permits):
        contractor_number = permit.get("contractor_number", "").strip()
        authority_normalized = permit.get("permit_authority_name_normalized", "").strip()
        permit_id = permit.get("permit_id", "").strip()

        logger.info(
            "[%d/%d] 確認開始: contractor_number=%s authority=%s",
            i + 1,
            len(permits),
            contractor_number,
            authority_normalized,
        )

        screenshot_path = get_screenshot_path(data_root, contractor_number or f"unknown_{i}")

        if dry_run:
            # dry-run: URL 生成のみ
            url = build_search_url(authority_normalized, contractor_number)
            pref_code = get_pref_code(authority_normalized)
            permit_type_code = get_permit_type_code(authority_normalized)
            logger.info(
                "[DRY-RUN] URL=%s type=%s pref=%s screenshot_path=%s",
                url,
                permit_type_code,
                pref_code,
                screenshot_path,
            )
            success += 1
            continue

        # Playwright で確認
        confirm_result = confirm_permit_with_playwright(permit, screenshot_path)
        logger.info(
            "確認結果: contractor_number=%s → %s (screenshot=%s)",
            contractor_number,
            confirm_result,
            screenshot_path,
        )

        # Sheets 書き戻し（sheets_client がある場合のみ）
        if sheets_client and permit_id:
            updated = update_permit_mlit_result(
                sheets_client,
                sheets_id,
                permit_id,
                today_str,
                confirm_result,
                str(screenshot_path),
                max_retries,
                base_delay,
            )
            if updated:
                logger.info("Sheets 更新完了: permit_id=%s", permit_id)
            else:
                logger.warning("Sheets 更新スキップ: permit_id=%s", permit_id)

        if confirm_result in ("一致", "不一致"):
            success += 1
        else:
            failed += 1

        # MLIT 規約対応: 1件ごとに 3秒ウェイト（最後の1件はウェイト不要）
        if i < len(permits) - 1:
            logger.debug("%.1f 秒ウェイト中...", MLIT_WAIT_SEC)
            time.sleep(MLIT_WAIT_SEC)

    print(
        f"\n=== MLIT 確認結果サマリー ===\n"
        f"  対象: {len(permits)} 件  成功: {success} 件  確認不可: {failed} 件"
    )


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MLIT etsuran2 で許可証の現在状態を確認する")
    p.add_argument("--csv", dest="csv_path", default=None, help="staging CSV ファイルパス（省略時は Sheets から取得）")
    p.add_argument("--dry-run", action="store_true", help="URL 生成のみ（Playwright 非実行）")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    main(
        csv_path=Path(args.csv_path) if args.csv_path else None,
        dry_run=args.dry_run,
    )
