#!/usr/bin/env python3
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

import sysbrowse  # noqa: E402


def run_sysmvp(repo_dir: Path, *args: str) -> None:
    subprocess.run(["python3", "sysmvp.py", *args], cwd=repo_dir, check=True)


def paths(rows) -> list[str]:
    return [str(row["current_path"] or row["canonical_uri"]) for row in rows]


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        repo_dir = Path(tmp) / "repo"
        repo_dir.mkdir()

        shutil.copy2(ROOT_DIR / "sysmvp.py", repo_dir / "sysmvp.py")
        shutil.copy2(ROOT_DIR / "schema.sql", repo_dir / "schema.sql")
        shutil.copy2(ROOT_DIR / ".sysignore", repo_dir / ".sysignore")

        run_sysmvp(repo_dir, "init")

        demo = repo_dir / "demo"
        demo.mkdir()
        (demo / "invoice-2025.txt").write_text("alpha\n", encoding="utf-8")
        (demo / "invoice-2024.txt").write_text("beta\n", encoding="utf-8")
        (demo / "notes.txt").write_text("invoice-2025 literal in metadata\n", encoding="utf-8")
        run_sysmvp(repo_dir, "scan", "--root", "demo")

        regex_rows = sysbrowse.fetch_files(repo_dir, r"invoice-202[45]\.txt$", "regex", "", "", "")
        assert paths(regex_rows) == ["demo/invoice-2024.txt", "demo/invoice-2025.txt"]

        text_rows = sysbrowse.fetch_files(repo_dir, r"invoice-202[45]\.txt$", "text", "", "", "")
        assert paths(text_rows) == []

        blob_rows = sysbrowse.fetch_blobs(repo_dir, r"objects/[0-9a-f]{2}/[0-9a-f]{64}$", "regex", "", "", "")
        assert len(blob_rows) == 3

        tx_rows = sysbrowse.fetch_transactions(repo_dir, r"^scan demo/invoice-202[45]\.txt$", "regex", "", "", "")
        assert len(tx_rows) == 2

        html = sysbrowse.render_files_partial(repo_dir, "[", "regex", "", "", "")
        assert "Invalid regex" in html

    print("[regex-search-smoke] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
