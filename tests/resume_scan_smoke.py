#!/usr/bin/env python3
import json
import sqlite3
import shutil
import subprocess
import tempfile
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent


def load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        repo_dir = Path(tmp) / "repo"
        repo_dir.mkdir()

        shutil.copy2(ROOT_DIR / "sysmvp.py", repo_dir / "sysmvp.py")
        shutil.copy2(ROOT_DIR / "schema.sql", repo_dir / "schema.sql")
        shutil.copy2(ROOT_DIR / ".sysignore", repo_dir / ".sysignore")

        subprocess.run(["python3", "sysmvp.py", "init"], cwd=repo_dir, check=True)

        scan_root = repo_dir / "longscan"
        ok_dir = scan_root / "01-ok"
        fail_dir = scan_root / "02-fail"
        later_dir = scan_root / "03-later"
        ok_dir.mkdir(parents=True)
        fail_dir.mkdir(parents=True)
        later_dir.mkdir(parents=True)

        ok_file = ok_dir / "a.txt"
        fail_file = fail_dir / "b.txt"
        later_file = later_dir / "c.txt"
        ok_file.write_text("alpha\n", encoding="utf-8")
        fail_file.write_text("blocked\n", encoding="utf-8")
        later_file.write_text("gamma\n", encoding="utf-8")

        fail_file.chmod(0)
        try:
            first_scan = subprocess.run(
                ["python3", "sysmvp.py", "scan", "--root", "longscan"],
                cwd=repo_dir,
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            )
        finally:
            fail_file.chmod(0o644)

        assert first_scan.returncode != 0, first_scan.stderr

        conn = sqlite3.connect(repo_dir / ".sysmvp.db")
        conn.row_factory = sqlite3.Row
        try:
            first_paths = [
                str(row["current_path"])
                for row in conn.execute(
                    "SELECT current_path FROM file_entry WHERE is_deleted = 0 ORDER BY current_path"
                ).fetchall()
            ]
            assert first_paths == ["longscan/01-ok/a.txt"], first_paths

            scan_run_count = int(conn.execute("SELECT COUNT(*) FROM scan_run").fetchone()[0])
            assert scan_run_count == 1, scan_run_count
        finally:
            conn.close()

        state_root = repo_dir / ".sysstore" / "scan_resume"
        state_dirs = [path for path in state_root.iterdir() if path.is_dir()]
        assert len(state_dirs) == 1, state_dirs
        state_dir = state_dirs[0]

        session = load_json(state_dir / "session.json")
        assert session["status"] == "failed", session

        trackers = {
            load_json(path)["dir"]: load_json(path)
            for path in (state_dir / "dirs").glob("*.json")
        }
        assert trackers["longscan"]["status"] == "done", trackers
        assert trackers["longscan/01-ok"]["status"] == "done", trackers
        assert trackers["longscan/02-fail"]["status"] == "failed", trackers
        assert "longscan/03-later" not in trackers, trackers

        subprocess.run(
            ["python3", "sysmvp.py", "scan", "--root", "longscan", "--resume"],
            cwd=repo_dir,
            check=True,
            stdout=subprocess.DEVNULL,
        )

        conn = sqlite3.connect(repo_dir / ".sysmvp.db")
        conn.row_factory = sqlite3.Row
        try:
            final_paths = [
                str(row["current_path"])
                for row in conn.execute(
                    "SELECT current_path FROM file_entry WHERE is_deleted = 0 ORDER BY current_path"
                ).fetchall()
            ]
            assert final_paths == [
                "longscan/01-ok/a.txt",
                "longscan/02-fail/b.txt",
                "longscan/03-later/c.txt",
            ], final_paths

            scan_run_count = int(conn.execute("SELECT COUNT(*) FROM scan_run").fetchone()[0])
            assert scan_run_count == 1, scan_run_count

            dir_marker_count = int(
                conn.execute("SELECT COUNT(*) FROM tx WHERE source = 'scan-dir'").fetchone()[0]
            )
            assert dir_marker_count == 4, dir_marker_count
        finally:
            conn.close()

        session = load_json(state_dir / "session.json")
        assert session["status"] == "completed", session

        trackers = {
            load_json(path)["dir"]: load_json(path)
            for path in (state_dir / "dirs").glob("*.json")
        }
        assert {payload["status"] for payload in trackers.values()} == {"done"}, trackers

        events = (state_dir / "events.ndjson").read_text(encoding="utf-8").splitlines()
        assert any('"event":"scan_failed"' in line for line in events), events
        assert any('"event":"scan_resume"' in line for line in events), events
        assert any('"event":"dir_skip_done"' in line for line in events), events

    print("[resume-scan-smoke] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
