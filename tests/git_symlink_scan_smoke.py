#!/usr/bin/env python3
import sqlite3
import shutil
import subprocess
import tempfile
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        repo_dir = Path(tmp) / "repo"
        repo_dir.mkdir()

        shutil.copy2(ROOT_DIR / "sysmvp.py", repo_dir / "sysmvp.py")
        shutil.copy2(ROOT_DIR / "schema.sql", repo_dir / "schema.sql")
        shutil.copy2(ROOT_DIR / ".sysignore", repo_dir / ".sysignore")

        subprocess.run(["python3", "sysmvp.py", "init"], cwd=repo_dir, check=True)

        scan_root = repo_dir / "gitroot"
        scan_root.mkdir()
        subprocess.run(["git", "init"], cwd=scan_root, check=True, stdout=subprocess.DEVNULL)
        target_file = scan_root / "target.txt"
        target_file.write_text("alpha\n", encoding="utf-8")
        (scan_root / "alias.txt").symlink_to(target_file.name)

        subprocess.run(
            ["python3", "sysmvp.py", "scan", "--root", "gitroot"],
            cwd=repo_dir,
            check=True,
            stdout=subprocess.DEVNULL,
        )

        conn = sqlite3.connect(repo_dir / ".sysmvp.db")
        try:
            git_rows = int(conn.execute("SELECT COUNT(*) FROM file_scan_git").fetchone()[0])
            distinct_git_files = int(conn.execute("SELECT COUNT(DISTINCT file_id) FROM file_scan_git").fetchone()[0])
            assert git_rows == distinct_git_files == 1, (git_rows, distinct_git_files)
        finally:
            conn.close()

    print("[git-symlink-scan-smoke] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
