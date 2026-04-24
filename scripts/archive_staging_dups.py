"""
staging/ 内の SHA256 重複ファイルをアーカイブ（move）するスクリプト。

- Type A: 再実行の _N 接尾辞版を archive
- Type B: 文字化け版を archive（正常デコード版を keep）
- Type C: ZIP展開 flat 名版を archive（TS接頭辞版を keep、provenance 保持）
- Type D: 再送の後発TS版を archive（最古TSを keep）
- Type E: UNKNOWN_xxx/ 配下を archive（会社確定版を keep）
- Type F: C0032 東海インプル建設 に重複している谷野宮組書類を archive

移動先: data/staging_archive/{YYYYMMDD}/<original_relative_path>
ログ:   logs/staging_archive_{YYYYMMDD}.csv
        + manifest_v2.csv（canonical_staged_path / provenance_note 列付き）
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from collections import defaultdict
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent
STAGING = PROJECT_ROOT / "data" / "staging"
ARCHIVE_ROOT = PROJECT_ROOT / "data" / "staging_archive"
LOG_DIR = PROJECT_ROOT / "logs"

TODAY_TAG = datetime.now().strftime("%Y%m%d")
ARCHIVE_DIR = ARCHIVE_ROOT / TODAY_TAG
MOVE_LOG = LOG_DIR / f"staging_archive_{TODAY_TAG}.csv"

MOJIBAKE_CHARS = set("üöîäâêÄÅ╧╩▓▒║▐▀┐╬┼╚└╝╗╕┘┴├├─│┤┬Üøôïí▬ë")
TS_PREFIX_RE = re.compile(r"^\d{8}_\d{6}_")
SUFFIX_N_RE = re.compile(r"_\d+$")


@dataclass
class FileInfo:
    path: Path
    size: int
    mtime: float
    name: str = ""
    has_mojibake: bool = False
    has_ts_prefix: bool = False
    has_suffix_n: bool = False
    ts_prefix: str = ""
    parent_name: str = ""
    is_in_unknown_dir: bool = False

    def __post_init__(self):
        self.name = self.path.name
        self.has_mojibake = any(c in self.name for c in MOJIBAKE_CHARS)
        m = TS_PREFIX_RE.match(self.name)
        if m:
            self.has_ts_prefix = True
            self.ts_prefix = m.group(0)
        self.has_suffix_n = bool(SUFFIX_N_RE.search(self.path.stem))
        self.parent_name = self.path.parent.name
        self.is_in_unknown_dir = self.parent_name.startswith("UNKNOWN_")


def compute_sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def scan_duplicates() -> dict[str, list[FileInfo]]:
    hashes: dict[str, list[FileInfo]] = defaultdict(list)
    for f in STAGING.rglob("*"):
        if not f.is_file() or f.name == "manifest.csv":
            continue
        try:
            h = compute_sha256(f)
        except Exception:
            continue
        st = f.stat()
        hashes[h].append(FileInfo(path=f, size=st.st_size, mtime=st.st_mtime))
    return {h: fs for h, fs in hashes.items() if len(fs) > 1}


def classify_group(files: list[FileInfo]) -> str:
    # F: C0032 東海インプル vs C0071 谷野宮組（特殊判定）
    parents = {f.parent_name for f in files}
    if any("C0032" in p for p in parents) and any("C0071" in p for p in parents):
        return "F_C0032_vs_C0071"

    # E: UNKNOWN_xxx と会社確定ディレクトリ混在
    if any(f.is_in_unknown_dir for f in files) and any(not f.is_in_unknown_dir for f in files):
        return "E_旧UNKNOWN残存"

    # B: 文字化け混在
    if any(f.has_mojibake for f in files) and any(not f.has_mojibake for f in files):
        return "B_文字化けvs正常"

    # D: 全員TSだが値が違う
    if all(f.has_ts_prefix for f in files):
        if len({f.ts_prefix for f in files}) >= 2:
            return "D_再送TS違い"

    # C: TSあり/なし混在
    if any(f.has_ts_prefix for f in files) and any(not f.has_ts_prefix for f in files):
        return "C_ZIP展開vs個別"

    # A: _N 接尾辞あり
    if any(f.has_suffix_n for f in files):
        return "A_再実行_N接尾辞"

    return "other"


def select_canonical(files: list[FileInfo], group_type: str) -> tuple[FileInfo, str]:
    if group_type == "F_C0032_vs_C0071":
        c0071 = [f for f in files if "C0071" in f.parent_name]
        return c0071[0], "谷野宮組(C0071)が書類の出所。東海インプル(C0032)は取引申請の受領側"

    if group_type == "E_旧UNKNOWN残存":
        confirmed = [f for f in files if not f.is_in_unknown_dir]
        return confirmed[0], f"会社確定版({confirmed[0].parent_name})を保持、旧UNKNOWN側をarchive"

    if group_type == "B_文字化けvs正常":
        clean = [f for f in files if not f.has_mojibake]
        return clean[0], "cp932正常デコード版を保持"

    if group_type == "D_再送TS違い":
        earliest = sorted(files, key=lambda f: f.ts_prefix)[0]
        return earliest, f"最古TS {earliest.ts_prefix} = 最初の受信"

    if group_type == "C_ZIP展開vs個別":
        ts_files = [f for f in files if f.has_ts_prefix]
        return ts_files[0], "TS接頭辞=メール受信provenance保持"

    if group_type == "A_再実行_N接尾辞":
        no_n = [f for f in files if not f.has_suffix_n]
        if no_n:
            return no_n[0], "_N接尾辞なし=最初のextract配置"
        return sorted(files, key=lambda f: f.name)[0], "全員_Nあり。名前順先頭"

    return sorted(files, key=lambda f: f.mtime)[0], "mtime最古"


def archive_file(src: Path, dry_run: bool) -> Path:
    """src をアーカイブへ移動。archive先パスを返す。"""
    rel = src.relative_to(STAGING)
    dest = ARCHIVE_DIR / rel
    if not dry_run:
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists():
            # 万一衝突した場合は _archX 付与
            i = 1
            while True:
                alt = dest.with_name(f"{dest.stem}_arch{i}{dest.suffix}")
                if not alt.exists():
                    dest = alt
                    break
                i += 1
        shutil.move(str(src), str(dest))
    return dest


def regenerate_manifest(move_log_rows: list[dict], dry_run: bool) -> None:
    """manifest_v2.csv を再生成（canonical_staged_path / provenance_note 列を追加）"""
    manifest_path = STAGING / "manifest.csv"
    new_manifest = STAGING / "manifest_v2.csv"
    if not manifest_path.exists():
        return

    # hash → canonical_staged_path マッピング
    hash_to_canonical: dict[str, str] = {}
    hash_to_archived: dict[str, list[str]] = defaultdict(list)
    for row in move_log_rows:
        h = row["file_hash"]
        if row["action"] == "keep":
            hash_to_canonical[h] = row["staged_path"]
        else:
            hash_to_archived[h].append(row["staged_path"])

    # 元manifest を読み、拡張列を付与
    with manifest_path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or []) + ["canonical_staged_path", "archived_from_group", "provenance_note"]

    for row in rows:
        h = row.get("file_hash", "")
        if h and h in hash_to_canonical:
            canonical = hash_to_canonical[h]
            row["canonical_staged_path"] = canonical
            if row["staged_path"] != canonical:
                row["archived_from_group"] = "true"
            else:
                row["archived_from_group"] = "false"
        else:
            row["canonical_staged_path"] = row.get("staged_path", "")
            row["archived_from_group"] = "false"
        row["provenance_note"] = ""

    if not dry_run:
        with new_manifest.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="実際には移動せず計画のみ")
    args = ap.parse_args()

    print(f"archive先: {ARCHIVE_DIR}")
    print(f"log:       {MOVE_LOG}")
    print(f"dry-run:   {args.dry_run}")
    print()

    dup_groups = scan_duplicates()
    print(f"重複グループ: {len(dup_groups)}")

    move_rows = []
    type_counts = defaultdict(lambda: {"groups": 0, "archived": 0})

    for h, files in dup_groups.items():
        gtype = classify_group(files)
        canonical, reason = select_canonical(files, gtype)
        type_counts[gtype]["groups"] += 1

        for f in files:
            if f.path == canonical.path:
                move_rows.append({
                    "file_hash": h,
                    "action": "keep",
                    "staged_path": str(f.path.relative_to(STAGING)),
                    "archive_path": "",
                    "group_type": gtype,
                    "canonical_reason": reason,
                })
            else:
                dest = archive_file(f.path, args.dry_run)
                type_counts[gtype]["archived"] += 1
                move_rows.append({
                    "file_hash": h,
                    "action": "archive",
                    "staged_path": str(f.path.relative_to(STAGING)),
                    "archive_path": str(dest.relative_to(PROJECT_ROOT)) if not args.dry_run else f"DRYRUN:{dest.relative_to(PROJECT_ROOT)}",
                    "group_type": gtype,
                    "canonical_reason": reason,
                })

    # ログ保存
    if not args.dry_run:
        LOG_DIR.mkdir(exist_ok=True)
        with MOVE_LOG.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["file_hash", "action", "staged_path", "archive_path", "group_type", "canonical_reason"])
            writer.writeheader()
            writer.writerows(move_rows)
        print(f"\n✓ ログ書き出し: {MOVE_LOG}")

    # manifest 再生成
    regenerate_manifest(move_rows, args.dry_run)
    if not args.dry_run:
        print(f"✓ manifest_v2.csv 生成: {STAGING / 'manifest_v2.csv'}")

    # サマリ
    print("\n=== タイプ別サマリ ===")
    for gtype, cnt in sorted(type_counts.items()):
        print(f"  {gtype}: {cnt['groups']}グループ / archive {cnt['archived']}件")

    total_archived = sum(c["archived"] for c in type_counts.values())
    print(f"\n合計 archive 対象: {total_archived} 件")


if __name__ == "__main__":
    main()
