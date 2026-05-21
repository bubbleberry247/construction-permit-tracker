"""
ASCII only portable delivery package.

Some customer PCs fail to open ZIPs that contain many Japanese file names.
This script creates a portable copy whose file and folder names are ASCII only,
then rewrites Excel hyperlinks to those portable paths.

Output:
  output/FDE_MANAGED_PORTABLE_<package_id>/
  output/delivery_zips/FDE_MANAGED_PORTABLE_<package_id>.zip

Usage:
  python scripts/build_ascii_portable_package.py --package-id 20260518_postal2
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import re
import shutil
import sys
import zipfile
from collections import defaultdict
from pathlib import Path

import openpyxl

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
OUTPUT = PROJECT / "output"
SRC = OUTPUT / "FDE_MANAGED"
ZIP_DIR = OUTPUT / "delivery_zips"

DEFAULT_REVIEW_XLSX = "00_一覧表_v3_20260518_郵送受領2社反映.xlsx"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def is_ascii_path(path: str) -> bool:
    try:
        path.encode("ascii")
        return True
    except UnicodeEncodeError:
        return False


def copy_pdf_tree(dest: Path, dedupe: bool = False) -> dict[str, str]:
    """Copy PDFs to ASCII paths and return old_rel -> new_rel map."""
    src_root = SRC / "10_originals"
    dest_root = dest / "10_originals"
    dest_root.mkdir(parents=True, exist_ok=True)

    per_company: dict[str, int] = defaultdict(int)
    hash_to_rel: dict[str, str] = {}
    mapping: dict[str, str] = {}

    for pdf in sorted(src_root.rglob("*.pdf")):
        rel = pdf.relative_to(SRC).as_posix()
        if dedupe:
            file_hash = sha256_file(pdf)
            existing_rel = hash_to_rel.get(file_hash)
            if existing_rel:
                mapping[rel] = existing_rel
                continue
        parts = rel.split("/")
        if len(parts) < 3:
            continue
        m = re.match(r"^(C\d{4})_", parts[1])
        cid = m.group(1) if m else "C0000"
        old_company = f"10_originals/{parts[1]}"
        new_company = f"10_originals/{cid}"
        mapping.setdefault(old_company, new_company)
        mapping.setdefault(old_company + "/", new_company + "/")
        per_company[cid] += 1
        new_name = f"{cid}_{per_company[cid]:03d}.pdf"
        new_path = dest_root / cid / new_name
        new_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(pdf, new_path)
        new_rel = new_path.relative_to(dest).as_posix()
        mapping[rel] = new_rel
        if dedupe:
            hash_to_rel[file_hash] = new_rel

    return mapping


def copy_index_files(dest: Path, mapping: dict[str, str]) -> None:
    src_index = SRC / "00_index"
    dest_index = dest / "00_index"
    dest_index.mkdir(parents=True, exist_ok=True)

    for src_file in src_index.glob("*"):
        if not src_file.is_file():
            continue
        if src_file.name == "index.csv":
            rows = []
            with src_file.open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames or []
                for row in reader:
                    rel = row.get("rel_path", "")
                    if rel in mapping:
                        row["rel_path"] = mapping[rel]
                    rows.append(row)
            with (dest_index / "index.csv").open("w", encoding="utf-8-sig", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(rows)
        else:
            shutil.copy2(src_file, dest_index / src_file.name)

    with (dest_index / "path_map.csv").open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["old_rel_path", "portable_rel_path", "sha256"])
        for old_rel, new_rel in sorted(mapping.items()):
            checksum = sha256_file(dest / new_rel) if (dest / new_rel).is_file() else ""
            writer.writerow([old_rel, new_rel, checksum])


def rewrite_hyperlink_target(target: str | None, mapping: dict[str, str]) -> str | None:
    if not target:
        return target
    base, sep, suffix = target.partition("#")
    base = base.replace("\\", "/")
    new_base = mapping.get(base)
    if not new_base:
        return target
    return new_base + (sep + suffix if sep else "")


def rewrite_workbook(src_xlsx: Path, dest_xlsx: Path, mapping: dict[str, str]) -> int:
    wb = openpyxl.load_workbook(src_xlsx)
    changed = 0
    for ws in wb.worksheets:
        headers = [c.value for c in ws[1]] if ws.max_row >= 1 else []
        rel_col = headers.index("rel_path") + 1 if "rel_path" in headers else None
        for row in ws.iter_rows():
            for cell in row:
                if cell.hyperlink and cell.hyperlink.target:
                    new_target = rewrite_hyperlink_target(cell.hyperlink.target, mapping)
                    if new_target != cell.hyperlink.target:
                        cell.hyperlink.target = new_target
                        changed += 1
                    if cell.hyperlink.tooltip:
                        cell.hyperlink.tooltip = None
                        changed += 1
                if rel_col and cell.column == rel_col and isinstance(cell.value, str):
                    old = cell.value.replace("\\", "/")
                    if old in mapping:
                        cell.value = mapping[old]
                        changed += 1
    wb.save(dest_xlsx)
    return changed


def resolve_review_xlsx(review_xlsx: str) -> Path:
    if review_xlsx:
        path = SRC / review_xlsx
        if not path.exists():
            raise FileNotFoundError(path)
        return path

    default_path = SRC / DEFAULT_REVIEW_XLSX
    if default_path.exists():
        return default_path

    candidates = sorted(SRC.glob("00_一覧表*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        raise FileNotFoundError(f"review xlsx not found under {SRC}")
    return candidates[0]


def write_manifest(dest: Path) -> None:
    entries = []
    for path in sorted(p for p in dest.rglob("*") if p.is_file()):
        rel = path.relative_to(dest).as_posix()
        entries.append((sha256_file(path), rel))
    with (dest / "manifest_sha256.txt").open("w", encoding="utf-8") as f:
        f.write("# Portable ASCII package SHA256 manifest\n")
        f.write(f"# total: {len(entries)} files\n\n")
        for sha, rel in entries:
            f.write(f"{sha}  {rel}\n")


def create_zip(dest: Path, package_id: str) -> Path:
    ZIP_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = ZIP_DIR / f"FDE_MANAGED_PORTABLE_{package_id}.zip"
    if zip_path.exists():
        zip_path.unlink()
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for path in sorted(p for p in dest.rglob("*") if p.is_file()):
            rel = path.relative_to(dest.parent).as_posix()
            zf.write(path, rel)
    return zip_path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--package-id", required=True)
    ap.add_argument("--review-xlsx", default="")
    ap.add_argument("--dedupe", action="store_true")
    args = ap.parse_args()

    dest = OUTPUT / f"FDE_MANAGED_PORTABLE_{args.package_id}"
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)

    mapping = copy_pdf_tree(dest, dedupe=args.dedupe)
    copy_index_files(dest, mapping)

    review_xlsx = resolve_review_xlsx(args.review_xlsx)
    dash_changes = rewrite_workbook(SRC / "dashboard.xlsx", dest / "dashboard.xlsx", mapping)
    review_changes = rewrite_workbook(review_xlsx, dest / "review_list.xlsx", mapping)

    readme = dest / "README_PORTABLE.txt"
    readme.write_text(
        f"""Portable package.

Use dashboard.xlsx or review_list.xlsx.
All file and folder names in this package are ASCII only.
Do not move Excel files out of this folder.
Duplicate PDF files are {'deduplicated and linked to one physical copy' if args.dedupe else 'kept as separate physical files'}.
""",
        encoding="utf-8",
    )
    write_manifest(dest)

    zip_path = create_zip(dest, args.package_id)
    with zipfile.ZipFile(zip_path) as zf:
        bad = zf.testzip()
        non_ascii = [n for n in zf.namelist() if not is_ascii_path(n)]

    print(f"portable folder: {dest}")
    print(f"portable zip: {zip_path}")
    print(f"review source: {review_xlsx}")
    print(f"pdf files: {len(mapping)}")
    print(f"physical pdf files: {len(list((dest / '10_originals').rglob('*.pdf')))}")
    print(f"dashboard links rewritten: {dash_changes}")
    print(f"review links rewritten: {review_changes}")
    print(f"zip test: {'ok' if bad is None else bad}")
    print(f"non-ascii zip entries: {len(non_ascii)}")
    print(f"zip size MB: {zip_path.stat().st_size / (1024 * 1024):.1f}")


if __name__ == "__main__":
    main()
