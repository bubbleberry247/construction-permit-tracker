"""
extract_inbox.py
data/inbox/ のZIP/LZH/その他ファイルを展開し、企業マッチングを行い、
data/staging/{company_id}_{company_name}/ へ配置 + manifest.csv を生成する。

処理フロー:
  inbox走査 → ZIP/LZH展開 → ファイル名から企業名抽出 → DB照合
  → staging配置 → SHA256重複チェック → manifest.csv出力

Usage:
    python src/extract_inbox.py            # 通常実行
    python src/extract_inbox.py --dry-run  # 展開・コピーせず結果を表示
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import re
import shutil
import sqlite3
import subprocess
import sys
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# パス定数
# ---------------------------------------------------------------------------
_SRC_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SRC_DIR.parent
_CONFIG_PATH = _PROJECT_ROOT / "config.json"
_DB_PATH = _PROJECT_ROOT / "data" / "permit_tracker.db"

# スキップ対象ファイル名パターン（大文字小文字無視）
_SKIP_PATTERNS: set[str] = {"icon.png", "image001.png", "image002.png", "thumbs.db", ".ds_store"}
_SKIP_EXTENSIONS: set[str] = {".png"}  # 画像ファイルはスキップ

# ---------------------------------------------------------------------------
# ログ設定
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# データクラス
# ---------------------------------------------------------------------------
@dataclass
class CompanyMatch:
    company_id: str
    company_name: str
    confidence: str  # HIGH, MEDIUM, LOW


@dataclass
class ManifestRow:
    source_file: str
    extracted_file: str
    staged_path: str
    company_id: str
    company_name: str
    confidence: str
    file_type: str
    file_hash: str
    duplicate: bool = False


# ---------------------------------------------------------------------------
# 設定読み込み
# ---------------------------------------------------------------------------
def load_config() -> dict[str, Any]:
    """config.json を読み込む"""
    with _CONFIG_PATH.open(encoding="utf-8") as f:
        return json.load(f)


def get_data_root(config: dict[str, Any]) -> Path:
    """DATA_ROOT を取得し Path に変換"""
    return Path(config.get("DATA_ROOT", str(_PROJECT_ROOT)))


# ---------------------------------------------------------------------------
# DB: 企業一覧読み込み
# ---------------------------------------------------------------------------
@dataclass
class CompanyRecord:
    company_id: str
    official_name: str


def load_companies(db_path: Path) -> list[CompanyRecord]:
    """companies テーブルから全企業を取得"""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT company_id, official_name FROM companies").fetchall()
    conn.close()
    return [CompanyRecord(r["company_id"], r["official_name"]) for r in rows]


# ---------------------------------------------------------------------------
# 企業名抽出 (ファイル名から)
# ---------------------------------------------------------------------------
def _strip_timestamp_prefix(name: str) -> str:
    """先頭の YYYYMMDD_HHMMSS_ タイムスタンプを除去"""
    m = re.match(r"^\d{8}_\d{6}_", name)
    return name[m.end():] if m else name


def _normalize_corp_suffix(name: str) -> str:
    """㈱ → 株式会社、(株) → 株式会社 等の正規化"""
    name = name.replace("㈱", "株式会社")
    name = name.replace("㈲", "有限会社")
    name = re.sub(r"[\(（]株[\)）]", "株式会社", name)
    name = re.sub(r"[\(（]有[\)）]", "有限会社", name)
    return name


def _is_generic_doc_name(name: str) -> bool:
    """企業名を含まない汎用書類名かを判定"""
    generic_words = [
        "チェックリスト", "会社案内", "工事経歴書", "労働安全",
        "労働者名簿", "資格者名簿", "提出書類", "建設業許可証",
        "取引先一覧", "御取引条件", "継続取引申請", "新規取引",
        "コピー御取引", "取引条件", "決算書", "誓約書",
    ]
    # 番号プレフィックス除去 (例: "3.1 建設業許可証")
    stripped = re.sub(r"^[\d.]+\s*", "", name).strip()
    return any(stripped.startswith(w) or stripped == w for w in generic_words)


def extract_company_hint(filename: str) -> str | None:
    """ファイル名から企業名ヒントを抽出する。

    パターン:
      - 御取引条件等説明書_株式会社イシハラ.zip → イシハラ
      - 中部建装㈱.zip → 中部建装株式会社
      - (株)あかり契約資料.zip → あかり
      - 御取引条件等説明書_尾畑長硝子(株).zip → 尾畑長硝子
      - 建設業許可証（谷野宮組）.pdf → 谷野宮組
      - 3.2 69期 ㈱大嶽安城 決算書.pdf → 大嶽安城
    """
    # タイムスタンプ除去
    name = _strip_timestamp_prefix(filename)
    # 拡張子除去 (.pdf.zip 対応)
    name = re.sub(r"(\.(pdf|zip|lzh|xlsx|png))+$", "", name, flags=re.IGNORECASE)

    # パターン0: 括弧内の企業名 — 「建設業許可証（谷野宮組）」「取引先一覧表（XXX）」
    m = re.search(r"[（(]([^)）]+?(?:組|工業|建設|電気|電工|工事|商店|建装|硝子|金属|木材|住宅|建築|防災|シャッター|システム|パーティション|ハウジング|トラスト|カンパニー|メンテック|サポート)[^)）]*?)[）)]", name)
    if m:
        return m.group(1).strip()
    # 括弧内にカタカナ/漢字の企業名らしいもの
    m = re.search(r"[（(]([^\x00-\x7F]{2,})[）)]", name)
    if m:
        candidate = m.group(1).strip()
        # 「東海インプル建設」のような自社名は除外
        if "東海インプル" not in candidate and "取引条件" not in candidate and "提出書類" not in candidate:
            return candidate

    # パターン1: 御取引条件等説明書_企業名
    m = re.search(r"御取引条件等説明書[＿_](.+)", name)
    if m:
        corp = _normalize_corp_suffix(m.group(1).strip())
        # 「取引条件・提出書類のご案内」等の汎用部分は除外
        if "取引条件" in corp or "提出書類" in corp:
            return None
        # 株式会社XXX → XXX を核心部分として返す
        core = re.sub(r"^(株式会社|有限会社)\s*", "", corp)
        core = re.sub(r"\s*(株式会社|有限会社)$", "", core)
        return core.strip() if core.strip() else corp

    # パターン2: ㈱XXX 決算書 / ㈱XXX 〜 — 法人格の直後の企業名を抽出
    normalized = _normalize_corp_suffix(name)
    m = re.search(r"(?:株式会社|有限会社)\s*(.+?)(?:\s+(?:決算書|契約|資料|提出|申請|書))", normalized)
    if m:
        return m.group(1).strip()

    # パターン2b: (株)XXX契約資料 — スペースなし版
    m = re.search(r"(?:株式会社|有限会社)\s*(.+?)(?:契約|資料|提出|申請)", normalized)
    if m:
        return m.group(1).strip()

    # パターン3: XXX㈱ / XXX(株) — 末尾に法人格
    m = re.search(r"(.+?)(?:㈱|㈲|[\(（]株[\)）]|[\(（]有[\)）]|株式会社|有限会社)", name)
    if m:
        # 御取引〜 のプレフィックスがある場合は除去
        candidate = m.group(1).strip()
        candidate = re.sub(r"^.*?[＿_]", "", candidate)
        # コピー等のプレフィックス除去
        candidate = re.sub(r"^コピー", "", candidate)
        # 数字プレフィックス除去 (例: "3.2 69期")
        candidate = re.sub(r"^[\d.]+\s*\d*期?\s*", "", candidate)
        if candidate:
            return candidate

    # パターン4: ファイル名そのもの（汎用書類名/数字のみ等を除外）
    if _is_generic_doc_name(name):
        return None
    if not re.match(r"^[\d\s._\-]+$", name) and "御取引" not in name and "継続取引" not in name and "コピー" not in name and "取引先" not in name and "東海インプル" not in name:
        # 日付/提出等の汎用ワードを含むか
        if not re.search(r"^\d{4}\.\d{2}\.\d{2}", name) and "提出" not in name and "新規" not in name:
            return name.strip()

    return None


# ---------------------------------------------------------------------------
# 企業マッチング
# ---------------------------------------------------------------------------
def match_company(
    hint: str | None,
    companies: list[CompanyRecord],
) -> CompanyMatch:
    """企業名ヒントから companies テーブルとマッチング"""
    if not hint:
        return CompanyMatch("UNKNOWN", "", "LOW")

    hint_normalized = hint.strip()

    # 完全一致
    for c in companies:
        if hint_normalized == c.official_name:
            return CompanyMatch(c.company_id, c.official_name, "HIGH")

    # 部分一致: ヒントが official_name に含まれる or 逆
    candidates: list[tuple[CompanyRecord, int]] = []
    for c in companies:
        if hint_normalized in c.official_name:
            candidates.append((c, len(hint_normalized)))
        elif c.official_name in hint_normalized:
            candidates.append((c, len(c.official_name)))

    if candidates:
        # 最も一致文字数が多いものを採用
        best = max(candidates, key=lambda x: x[1])
        return CompanyMatch(best[0].company_id, best[0].official_name, "MEDIUM")

    # 法人格を除いた核心部分で再マッチ
    core_hint = re.sub(r"(株式会社|有限会社|㈱|㈲)", "", hint_normalized).strip()
    if core_hint and core_hint != hint_normalized:
        for c in companies:
            core_official = re.sub(r"(株式会社|有限会社|㈱|㈲)", "", c.official_name).strip()
            if core_hint in core_official or core_official in core_hint:
                return CompanyMatch(c.company_id, c.official_name, "MEDIUM")

    return CompanyMatch("UNKNOWN", "", "LOW")


# ---------------------------------------------------------------------------
# SHA256 ハッシュ
# ---------------------------------------------------------------------------
def compute_sha256(file_path: Path) -> str:
    """ファイルの SHA256 ハッシュを計算"""
    h = hashlib.sha256()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# 重複チェック (originals/ 内の既存ファイル)
# ---------------------------------------------------------------------------
def build_originals_hash_set(originals_dir: Path) -> set[str]:
    """data/originals/ 配下の全ファイルハッシュを収集"""
    hashes: set[str] = set()
    if not originals_dir.exists():
        return hashes
    for f in originals_dir.rglob("*"):
        if f.is_file():
            try:
                hashes.add(compute_sha256(f))
            except OSError as e:
                logger.warning("ハッシュ計算失敗: %s (%s)", f, e)
    return hashes


# ---------------------------------------------------------------------------
# ZIP 展開
# ---------------------------------------------------------------------------
def _should_skip_file(name: str) -> bool:
    """展開対象外のファイルか判定"""
    basename = Path(name).name.lower()
    if basename in {s.lower() for s in _SKIP_PATTERNS}:
        return True
    if Path(basename).suffix.lower() in _SKIP_EXTENSIONS:
        return True
    # ディレクトリエントリ（末尾 / ）
    if name.endswith("/") or name.endswith("\\"):
        return True
    return False


def _decode_zip_filename(info: zipfile.ZipInfo) -> str:
    """ZIP内ファイル名を正しくデコード（cp932 フォールバック）"""
    if info.flag_bits & 0x800:
        # UTF-8 フラグ
        return info.filename
    try:
        # Python の zipfile は cp437 でデコードするが、日本語Windowsは cp932
        raw = info.filename.encode("cp437")
        return raw.decode("cp932")
    except (UnicodeDecodeError, UnicodeEncodeError):
        return info.filename


def extract_zip(
    zip_path: Path,
    staging_dir: Path,
    company_match: CompanyMatch,
    dry_run: bool = False,
) -> list[tuple[str, Path]]:
    """ZIP を展開し (元ファイル名, 展開先Path) のリストを返す。
    フラット化: ネストされたフォルダ構造を無視し、ファイルのみ取り出す。
    """
    results: list[tuple[str, Path]] = []

    try:
        with zipfile.ZipFile(str(zip_path), "r") as zf:
            for info in zf.infolist():
                decoded_name = _decode_zip_filename(info)

                # ディレクトリ / スキップ対象
                if info.is_dir() or _should_skip_file(decoded_name):
                    logger.debug("スキップ: %s", decoded_name)
                    continue

                # フラット化: パスの最後の要素のみ使用
                flat_name = Path(decoded_name).name
                if not flat_name:
                    continue

                # staging ディレクトリ決定
                if company_match.company_id != "UNKNOWN":
                    dest_dir = staging_dir / f"{company_match.company_id}_{company_match.company_name}"
                else:
                    dest_dir = staging_dir / f"UNKNOWN_{zip_path.stem}"

                dest_path = dest_dir / flat_name

                # 同名ファイル回避
                if dest_path.exists():
                    stem = dest_path.stem
                    suffix = dest_path.suffix
                    counter = 1
                    while dest_path.exists():
                        dest_path = dest_dir / f"{stem}_{counter}{suffix}"
                        counter += 1

                if not dry_run:
                    dest_dir.mkdir(parents=True, exist_ok=True)
                    data = zf.read(info.filename)
                    dest_path.write_bytes(data)
                    logger.info("  展開: %s → %s", decoded_name, dest_path.name)

                results.append((flat_name, dest_path))
    except zipfile.BadZipFile as e:
        logger.error("不正なZIP: %s (%s)", zip_path.name, e)
    except Exception as e:
        logger.error("ZIP展開エラー: %s (%s)", zip_path.name, e)

    return results


# ---------------------------------------------------------------------------
# ZIP内ファイル名からの企業ヒント抽出
# ---------------------------------------------------------------------------
def extract_hints_from_zip_contents(zip_path: Path) -> str | None:
    """ZIP内のファイル名リストから企業名ヒントを探す"""
    try:
        with zipfile.ZipFile(str(zip_path), "r") as zf:
            for info in zf.infolist():
                decoded = _decode_zip_filename(info)
                if info.is_dir():
                    continue
                # フォルダ名が「東海インプル建設」の場合は宛先なのでスキップ
                if "東海インプル" in decoded:
                    continue
                hint = extract_company_hint(Path(decoded).name)
                if hint:
                    return hint
    except (zipfile.BadZipFile, Exception):
        pass
    return None


# ---------------------------------------------------------------------------
# LZH 展開
# ---------------------------------------------------------------------------
def extract_lzh(
    lzh_path: Path,
    staging_dir: Path,
    company_match: CompanyMatch,
    dry_run: bool = False,
) -> list[tuple[str, Path]]:
    """LZH を展開。lhafile パッケージ → 7z フォールバック → スキップ"""
    results: list[tuple[str, Path]] = []

    if company_match.company_id != "UNKNOWN":
        dest_dir = staging_dir / f"{company_match.company_id}_{company_match.company_name}"
    else:
        dest_dir = staging_dir / f"UNKNOWN_{lzh_path.stem}"

    # --- lhafile パッケージ ---
    try:
        import lhafile  # type: ignore[import-untyped]

        with lhafile.Lhafile(str(lzh_path)) as lf:
            for name in lf.namelist():
                if _should_skip_file(name):
                    continue
                flat_name = Path(name).name
                if not flat_name:
                    continue
                dest_path = dest_dir / flat_name
                if not dry_run:
                    dest_dir.mkdir(parents=True, exist_ok=True)
                    dest_path.write_bytes(lf.read(name))
                    logger.info("  LZH展開: %s → %s", name, dest_path.name)
                results.append((flat_name, dest_path))
        return results
    except ImportError:
        logger.debug("lhafile パッケージ未インストール、7z フォールバックを試行")
    except Exception as e:
        logger.warning("lhafile でのLZH展開失敗: %s (%s)", lzh_path.name, e)

    # --- 7z フォールバック ---
    seven_zip_paths = [
        "7z",
        "C:/Program Files/7-Zip/7z.exe",
        "C:/Program Files (x86)/7-Zip/7z.exe",
    ]
    seven_zip_cmd: str | None = None
    for p in seven_zip_paths:
        try:
            subprocess.run(
                [p, "--help"],
                capture_output=True,
                timeout=5,
            )
            seven_zip_cmd = p
            break
        except (FileNotFoundError, subprocess.TimeoutExpired):
            continue

    if seven_zip_cmd and not dry_run:
        try:
            temp_dir = staging_dir / f"_tmp_lzh_{lzh_path.stem}"
            temp_dir.mkdir(parents=True, exist_ok=True)
            subprocess.run(
                [seven_zip_cmd, "x", "-y", f"-o{temp_dir}", str(lzh_path)],
                capture_output=True,
                timeout=60,
                check=True,
            )
            dest_dir.mkdir(parents=True, exist_ok=True)
            for f in temp_dir.rglob("*"):
                if f.is_file() and not _should_skip_file(f.name):
                    dest_path = dest_dir / f.name
                    shutil.move(str(f), str(dest_path))
                    logger.info("  LZH(7z)展開: %s → %s", f.name, dest_path.name)
                    results.append((f.name, dest_path))
            shutil.rmtree(str(temp_dir), ignore_errors=True)
            return results
        except subprocess.CalledProcessError as e:
            logger.warning("7z でのLZH展開失敗: %s (%s)", lzh_path.name, e)
            shutil.rmtree(str(temp_dir), ignore_errors=True)
        except Exception as e:
            logger.warning("7z LZH展開エラー: %s (%s)", lzh_path.name, e)
    elif seven_zip_cmd and dry_run:
        # dry-run: 7z l でリスト取得
        try:
            result = subprocess.run(
                [seven_zip_cmd, "l", str(lzh_path)],
                capture_output=True,
                text=True,
                timeout=10,
            )
            for line in result.stdout.splitlines():
                # 7z l 出力のファイル行をパース（簡易）
                parts = line.strip().split()
                if parts and not line.startswith("---") and len(parts) >= 6:
                    name = parts[-1]
                    if not _should_skip_file(name) and "." in name:
                        flat_name = Path(name).name
                        dest_path = dest_dir / flat_name
                        results.append((flat_name, dest_path))
        except Exception:
            pass

    if not results:
        logger.warning(
            "LZH展開不可 (lhafile未インストール / 7z未検出): %s — 手動展開が必要です",
            lzh_path.name,
        )

    return results


# ---------------------------------------------------------------------------
# 非アーカイブファイル処理
# ---------------------------------------------------------------------------
def process_loose_file(
    file_path: Path,
    staging_dir: Path,
    company_match: CompanyMatch,
    dry_run: bool = False,
) -> tuple[str, Path] | None:
    """ZIP/LZH 以外の単体ファイルを staging にコピー"""
    if _should_skip_file(file_path.name):
        logger.debug("スキップ (単体ファイル): %s", file_path.name)
        return None

    if company_match.company_id != "UNKNOWN":
        dest_dir = staging_dir / f"{company_match.company_id}_{company_match.company_name}"
    else:
        dest_dir = staging_dir / f"UNKNOWN_{file_path.stem}"

    dest_path = dest_dir / file_path.name

    # 同名ファイル回避
    if dest_path.exists():
        stem = dest_path.stem
        suffix = dest_path.suffix
        counter = 1
        while dest_path.exists():
            dest_path = dest_dir / f"{stem}_{counter}{suffix}"
            counter += 1

    if not dry_run:
        dest_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(file_path), str(dest_path))
        logger.info("  コピー: %s → %s", file_path.name, dest_path.name)

    return (file_path.name, dest_path)


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------
def run_extraction(
    inbox_dir: Path,
    staging_dir: Path,
    originals_dir: Path,
    db_path: Path,
    dry_run: bool = False,
) -> list[ManifestRow]:
    """inbox 全ファイルを処理し ManifestRow のリストを返す"""

    # 企業マスタ読み込み
    companies = load_companies(db_path)
    logger.info("企業マスタ: %d社ロード済み", len(companies))

    # originals ハッシュセット構築
    logger.info("originals/ ハッシュセット構築中...")
    originals_hashes = build_originals_hash_set(originals_dir)
    logger.info("originals/ ファイル数: %d", len(originals_hashes))

    # inbox ファイル一覧
    if not inbox_dir.exists():
        logger.error("inbox ディレクトリが存在しません: %s", inbox_dir)
        return []

    inbox_files = sorted(inbox_dir.iterdir())
    if not inbox_files:
        logger.info("inbox にファイルがありません")
        return []

    logger.info("inbox ファイル数: %d", len(inbox_files))

    manifest: list[ManifestRow] = []

    for inbox_file in inbox_files:
        if not inbox_file.is_file():
            continue

        source_name = inbox_file.name
        suffix_lower = inbox_file.suffix.lower()

        logger.info("処理中: %s", source_name)

        # --- 企業マッチング ---
        hint = extract_company_hint(source_name)
        company = match_company(hint, companies)

        # ZIP の場合: 中身のファイル名からもヒントを探す
        if suffix_lower == ".zip" and company.company_id == "UNKNOWN":
            inner_hint = extract_hints_from_zip_contents(inbox_file)
            if inner_hint:
                company = match_company(inner_hint, companies)
                if company.company_id != "UNKNOWN":
                    logger.info("  ZIP内ファイル名からマッチ: %s → %s", inner_hint, company.company_id)

        logger.info(
            "  企業マッチ: %s (%s) [%s]",
            company.company_id,
            company.company_name,
            company.confidence,
        )

        # --- 展開 / コピー ---
        extracted_files: list[tuple[str, Path]] = []

        if suffix_lower == ".zip":
            extracted_files = extract_zip(inbox_file, staging_dir, company, dry_run)

        elif suffix_lower == ".lzh":
            extracted_files = extract_lzh(inbox_file, staging_dir, company, dry_run)

        else:
            result = process_loose_file(inbox_file, staging_dir, company, dry_run)
            if result:
                extracted_files = [result]

        # --- Manifest 作成 ---
        for extracted_name, staged_path in extracted_files:
            file_ext = Path(extracted_name).suffix.lstrip(".").upper()
            if not file_ext:
                file_ext = "UNKNOWN"

            # ハッシュ計算
            if not dry_run and staged_path.exists():
                file_hash = compute_sha256(staged_path)
            else:
                file_hash = ""

            # 重複チェック
            is_duplicate = file_hash in originals_hashes if file_hash else False

            row = ManifestRow(
                source_file=source_name,
                extracted_file=extracted_name,
                staged_path=str(staged_path),
                company_id=company.company_id,
                company_name=company.company_name,
                confidence=company.confidence,
                file_type=file_ext,
                file_hash=file_hash,
                duplicate=is_duplicate,
            )
            manifest.append(row)

            if is_duplicate:
                logger.info("  ⚠ 重複検出: %s", extracted_name)

    return manifest


def write_manifest(manifest: list[ManifestRow], manifest_path: Path) -> None:
    """manifest.csv を書き出す"""
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "source_file",
        "extracted_file",
        "staged_path",
        "company_id",
        "company_name",
        "confidence",
        "file_type",
        "file_hash",
        "duplicate",
    ]

    with manifest_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in manifest:
            writer.writerow({
                "source_file": row.source_file,
                "extracted_file": row.extracted_file,
                "staged_path": row.staged_path,
                "company_id": row.company_id,
                "company_name": row.company_name,
                "confidence": row.confidence,
                "file_type": row.file_type,
                "file_hash": row.file_hash,
                "duplicate": row.duplicate,
            })

    logger.info("manifest.csv 出力: %s (%d行)", manifest_path, len(manifest))


def print_summary(manifest: list[ManifestRow]) -> None:
    """処理結果のサマリーを表示"""
    total = len(manifest)
    matched = sum(1 for r in manifest if r.company_id != "UNKNOWN")
    unknown = total - matched
    duplicates = sum(1 for r in manifest if r.duplicate)

    # 企業別集計
    by_company: dict[str, int] = {}
    for r in manifest:
        key = f"{r.company_id} {r.company_name}" if r.company_id != "UNKNOWN" else "UNKNOWN"
        by_company[key] = by_company.get(key, 0) + 1

    logger.info("=" * 60)
    logger.info("処理結果サマリー")
    logger.info("  総ファイル数:   %d", total)
    logger.info("  マッチ済み:     %d", matched)
    logger.info("  UNKNOWN:        %d", unknown)
    logger.info("  重複 (originals): %d", duplicates)
    logger.info("-" * 60)
    for key in sorted(by_company.keys()):
        logger.info("  %-40s %d件", key, by_company[key])
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# エントリーポイント
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="data/inbox/ のファイルを展開・企業マッチングし staging/ に配置",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="展開・コピーを行わず、マッチング結果のみ表示",
    )
    args = parser.parse_args()

    config = load_config()
    data_root = get_data_root(config)

    inbox_dir = data_root / "data" / "inbox"
    staging_dir = data_root / "data" / "staging"
    originals_dir = data_root / "data" / "originals"
    manifest_path = staging_dir / "manifest.csv"

    logger.info("inbox:    %s", inbox_dir)
    logger.info("staging:  %s", staging_dir)
    logger.info("originals: %s", originals_dir)
    logger.info("DB:       %s", _DB_PATH)
    if args.dry_run:
        logger.info("*** DRY-RUN モード ***")

    manifest = run_extraction(
        inbox_dir=inbox_dir,
        staging_dir=staging_dir,
        originals_dir=originals_dir,
        db_path=_DB_PATH,
        dry_run=args.dry_run,
    )

    if manifest:
        if not args.dry_run:
            write_manifest(manifest, manifest_path)
        print_summary(manifest)
    else:
        logger.info("処理対象ファイルなし")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
    main()
