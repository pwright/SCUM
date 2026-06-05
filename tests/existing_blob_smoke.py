#!/usr/bin/env python3
import hashlib
import shutil
import sqlite3
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


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        repo_dir = Path(tmp) / "repo"
        repo_dir.mkdir()

        shutil.copy2(ROOT_DIR / "sysmvp.py", repo_dir / "sysmvp.py")
        shutil.copy2(ROOT_DIR / "schema.sql", repo_dir / "schema.sql")
        shutil.copy2(ROOT_DIR / ".sysignore", repo_dir / ".sysignore")

        run_sysmvp(repo_dir, "init")

        source_dir = repo_dir / "docs"
        source_dir.mkdir()
        source_file = source_dir / "existing.txt"
        source_file.write_text("reattach me\n", encoding="utf-8")
        blob_hash = hashlib.sha256(source_file.read_bytes()).hexdigest()

        object_path = repo_dir / ".sysstore" / "objects" / blob_hash[:2] / blob_hash
        object_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_file, object_path)
        object_path.chmod(0o444)

        result = run_sysmvp(repo_dir, "scan", "--root", "docs")
        assert f"Reattached existing blob {blob_hash[:12]}" in result.stderr

        conn = sqlite3.connect(repo_dir / ".sysmvp.db")
        try:
            row = conn.execute("SELECT size_bytes FROM blob_object WHERE blob_hash = ?", (blob_hash,)).fetchone()
            assert row is not None
            assert int(row[0]) == len("reattach me\n")
        finally:
            conn.close()
            object_path.chmod(0o644)

    print("[existing-blob-smoke] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
