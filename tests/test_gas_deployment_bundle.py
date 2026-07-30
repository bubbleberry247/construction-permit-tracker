from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "build_gas_deployment_bundle.py"
SPEC = importlib.util.spec_from_file_location("build_gas_deployment_bundle", SCRIPT_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def make_source(tmp_path: Path) -> Path:
    source = tmp_path / "src"
    source.mkdir()
    (source / "Code.gs").write_text("function doGet() {}\n", encoding="utf-8")
    (source / "index.html").write_text("<p>test</p>\n", encoding="utf-8")
    (source / "ignored.py").write_text("raise SystemExit\n", encoding="utf-8")
    (source / "appsscript.json").write_text(
        json.dumps(
            {
                "timeZone": "Asia/Tokyo",
                "webapp": {"access": "MYSELF", "executeAs": "USER_DEPLOYING"},
            }
        ),
        encoding="utf-8",
    )
    return source


@pytest.mark.parametrize(
    ("profile", "expected_access"),
    [
        ("UAT", "ANYONE"),
        ("PRODUCTION_PRIVATE", "MYSELF"),
        ("PRODUCTION_PUBLIC", "ANYONE"),
    ],
)
def test_profile_builds_bound_bundle(
    tmp_path: Path, profile: str, expected_access: str
) -> None:
    source = make_source(tmp_path)
    output = tmp_path / f"bundle-{profile}"
    before = (source / "appsscript.json").read_bytes()
    metadata = MODULE.build_bundle(
        source,
        output,
        profile,
        "A" * 30,
        "B" * 30,
    )

    manifest = json.loads((output / "appsscript.json").read_text(encoding="utf-8"))
    clasp = json.loads((output / ".clasp.json").read_text(encoding="utf-8"))
    assert manifest["webapp"] == {
        "access": expected_access,
        "executeAs": "USER_DEPLOYING",
    }
    assert manifest["webapp"]["access"] != "ANYONE_ANONYMOUS"
    assert clasp["scriptId"] == "A" * 30
    assert metadata["spreadsheetId"] == "B" * 30
    assert not (output / "ignored.py").exists()
    assert (source / "appsscript.json").read_bytes() == before


def test_refuses_public_tracked_manifest(tmp_path: Path) -> None:
    source = make_source(tmp_path)
    manifest_path = source / "appsscript.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["webapp"]["access"] = "ANYONE"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="must remain MYSELF"):
        MODULE.build_bundle(
            source,
            tmp_path / "bundle",
            "UAT",
            "A" * 30,
            "B" * 30,
        )


def test_refuses_nonempty_output(tmp_path: Path) -> None:
    source = make_source(tmp_path)
    output = tmp_path / "bundle"
    output.mkdir()
    (output / "keep.txt").write_text("user file", encoding="utf-8")
    with pytest.raises(ValueError, match="absent or empty"):
        MODULE.build_bundle(
            source,
            output,
            "UAT",
            "A" * 30,
            "B" * 30,
        )
