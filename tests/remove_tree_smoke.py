#!/usr/bin/env python3
import json
import sqlite3
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

        scan_root = repo_dir / "examples" / "demo"
        subdir = scan_root / "subdir"
        sibling = scan_root / "sibling"
        subdir.mkdir(parents=True)
        sibling.mkdir()
        (scan_root / "top.txt").write_text("top\n", encoding="utf-8")
        (subdir / "a.txt").write_text("alpha\n", encoding="utf-8")
        (subdir / "b.txt").write_text("beta\n", encoding="utf-8")
        (sibling / "c.txt").write_text("gamma\n", encoding="utf-8")

        run_sysmvp(repo_dir, "scan", "--root", "examples/demo")
        assert listed_paths(repo_dir) == [
            "examples/demo/top.txt",
            "examples/demo/sibling/c.txt",
            "examples/demo/subdir/a.txt",
            "examples/demo/subdir/b.txt",
        ]

        run_sysmvp(repo_dir, "remove", "examples/demo/subdir")
        assert listed_paths(repo_dir) == [
            "examples/demo/top.txt",
            "examples/demo/sibling/c.txt",
        ]

        conn = sqlite3.connect(repo_dir / ".sysmvp.db")
        conn.row_factory = sqlite3.Row
        try:
            removed_rows = conn.execute(
                """
                SELECT file_id, current_path
                FROM file_entry
                WHERE current_path LIKE 'examples/demo/subdir/%'
                ORDER BY current_path
                """
            ).fetchall()
            assert [int(row["file_id"]) for row in removed_rows]
            assert all(
                int(
                    conn.execute(
                        """
                        SELECT COUNT(*)
                        FROM fact f
                        JOIN tx t ON t.tx_id = f.tx_id
                        WHERE t.source = 'remove'
                          AND f.entity_id = ?
                          AND f.added = 0
                        """,
                        (int(row["file_id"]),),
                    ).fetchone()[0]
                )
                > 0
                for row in removed_rows
            )
        finally:
            conn.close()

        run_sysmvp(repo_dir, "scan", "--root", "examples/demo")
        assert listed_paths(repo_dir) == [
            "examples/demo/top.txt",
            "examples/demo/sibling/c.txt",
            "examples/demo/subdir/a.txt",
            "examples/demo/subdir/b.txt",
        ]

    print("[remove-tree-smoke] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
