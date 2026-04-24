"""
staging/ のPDFを originals/ へコピー。
hash ベースで重複は自動スキップ。
"""
import hashlib
import shutil
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
STAGING = PROJECT_ROOT / "data" / "staging"
ORIGINALS = PROJECT_ROOT / "data" / "originals"


def sha256_of(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def build_originals_hash_index() -> set[str]:
    return {sha256_of(f) for f in ORIGINALS.rglob("*") if f.is_file()}


def main():
    ORIGINALS.mkdir(exist_ok=True)
    existing = build_originals_hash_index()
    print(f"originals 既存 hash: {len(existing)}")

    copied, skipped = 0, 0
    by_company = {}
    for cdir in sorted(STAGING.iterdir()):
        if not cdir.is_dir():
            continue
        dest_cdir = ORIGINALS / cdir.name
        for f in cdir.iterdir():
            if not f.is_file() or f.suffix.lower() != ".pdf":
                continue
            h = sha256_of(f)
            if h in existing:
                skipped += 1
                continue
            dest_cdir.mkdir(parents=True, exist_ok=True)
            dest = dest_cdir / f.name
            if dest.exists():
                # 同名別内容なら _1 付与
                i = 1
                while True:
                    alt = dest_cdir / f"{f.stem}_{i}{f.suffix}"
                    if not alt.exists():
                        dest = alt
                        break
                    i += 1
            shutil.copy2(str(f), str(dest))
            existing.add(h)
            copied += 1
            by_company.setdefault(cdir.name, 0)
            by_company[cdir.name] += 1

    print(f"\nコピー: {copied}件")
    print(f"スキップ (hash重複): {skipped}件")
    print("\n=== 会社別コピー件数 ===")
    for c, n in sorted(by_company.items()):
        print(f"  {c}: {n}件")


if __name__ == "__main__":
    main()
