#!/usr/bin/env python3
"""Build an environment-bound Apps Script deployment bundle.

The tracked src/appsscript.json must remain owner-only. This builder creates a
separate bundle for UAT or an explicitly approved production release and binds
the bundle to an exact Apps Script project ID and Spreadsheet ID.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path


PROFILES = {
    "UAT": {"access": "ANYONE", "executeAs": "USER_DEPLOYING"},
    "PRODUCTION_PRIVATE": {"access": "MYSELF", "executeAs": "USER_DEPLOYING"},
    "PRODUCTION_PUBLIC": {"access": "ANYONE", "executeAs": "USER_DEPLOYING"},
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest().upper()


def validate_identifier(value: str, label: str) -> str:
    normalized = value.strip()
    if not normalized or len(normalized) < 20 or len(normalized) > 200:
        raise ValueError(f"{label} is missing or invalid")
    if any(character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-" for character in normalized):
        raise ValueError(f"{label} contains invalid characters")
    return normalized


def build_bundle(
    source_dir: Path,
    output_dir: Path,
    profile: str,
    script_id: str,
    spreadsheet_id: str,
) -> dict[str, object]:
    source_dir = source_dir.resolve()
    output_dir = output_dir.resolve()
    normalized_profile = profile.strip().upper()
    if normalized_profile not in PROFILES:
        raise ValueError(f"unknown profile: {profile}")
    if source_dir == output_dir or source_dir in output_dir.parents:
        # Output below src could be uploaded accidentally by clasp.
        raise ValueError("output directory must not be src or a child of src")
    if output_dir.exists() and any(output_dir.iterdir()):
        raise ValueError("output directory must be absent or empty")

    manifest_path = source_dir / "appsscript.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    tracked_webapp = manifest.get("webapp") or {}
    if tracked_webapp.get("access") != "MYSELF":
        raise ValueError("tracked appsscript.json must remain MYSELF")
    if tracked_webapp.get("executeAs") != "USER_DEPLOYING":
        raise ValueError("tracked appsscript.json must remain USER_DEPLOYING")

    script_id = validate_identifier(script_id, "script ID")
    spreadsheet_id = validate_identifier(spreadsheet_id, "spreadsheet ID")
    output_dir.mkdir(parents=True, exist_ok=True)

    copied: list[Path] = []
    for source in sorted(source_dir.iterdir()):
        if source.name == "appsscript.json":
            continue
        if source.suffix.lower() not in {".gs", ".html"}:
            continue
        destination = output_dir / source.name
        shutil.copy2(source, destination)
        copied.append(destination)

    release_manifest = dict(manifest)
    release_manifest["webapp"] = dict(PROFILES[normalized_profile])
    generated_manifest = output_dir / "appsscript.json"
    generated_manifest.write_text(
        json.dumps(release_manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    copied.append(generated_manifest)

    (output_dir / ".clasp.json").write_text(
        json.dumps({"scriptId": script_id, "rootDir": "."}, indent=2) + "\n",
        encoding="utf-8",
    )

    hashes = {path.name: sha256_file(path) for path in sorted(copied)}
    metadata = {
        "profile": normalized_profile,
        "scriptId": script_id,
        "spreadsheetId": spreadsheet_id,
        "webapp": release_manifest["webapp"],
        "sourceDir": str(source_dir),
        "fileCount": len(copied),
        "sha256": hashes,
    }
    (output_dir / "deployment-metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return metadata


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True, choices=sorted(PROFILES))
    parser.add_argument("--script-id", required=True)
    parser.add_argument("--spreadsheet-id", required=True)
    parser.add_argument("--source-dir", type=Path, default=Path("src"))
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    metadata = build_bundle(
        source_dir=args.source_dir,
        output_dir=args.output_dir,
        profile=args.profile,
        script_id=args.script_id,
        spreadsheet_id=args.spreadsheet_id,
    )
    print(json.dumps(metadata, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
