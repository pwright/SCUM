#!/usr/bin/env python3
import json
import shutil
import subprocess
import tempfile
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent


def run_sysmvp(repo_dir: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["python3", "sysmvp.py", *args],
        cwd=repo_dir,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def listed_paths(repo_dir: Path) -> list[str]:
    result = run_sysmvp(repo_dir, "list", "--json")
    return [str(item["current_path"]) for item in json.loads(result.stdout)]


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        repo_dir = Path(tmp) / "repo"
        repo_dir.mkdir()

        shutil.copy2(ROOT_DIR / "sysmvp.py", repo_dir / "sysmvp.py")
        shutil.copy2(ROOT_DIR / "schema.sql", repo_dir / "schema.sql")
        shutil.copy2(ROOT_DIR / ".sysignore", repo_dir / ".sysignore")

        run_sysmvp(repo_dir, "init")

        scan_root = repo_dir / "docs"
        scan_root.mkdir()
        (scan_root / "small.txt").write_text("12345", encoding="utf-8")
        (scan_root / "large.txt").write_text("123456", encoding="utf-8")

        scan_result = run_sysmvp(repo_dir, "scan", "--max", "5", "docs")
        assert listed_paths(repo_dir) == ["docs/small.txt"]
        assert "Skipping docs/large.txt: size=6 exceeds max=5" in scan_result.stderr

        single_result = run_sysmvp(repo_dir, "scan", "--file", "docs/large.txt", "--max", "5b")
        assert listed_paths(repo_dir) == ["docs/small.txt"]
        assert "Skipping docs/large.txt: size=6 exceeds max=5" in single_result.stderr

        run_sysmvp(repo_dir, "scan", "--max", "1k", "docs")
        assert listed_paths(repo_dir) == ["docs/small.txt", "docs/large.txt"]

    print("[max-size-scan-smoke] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
