#!/usr/bin/env python3
import os
import sys
import tempfile
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

import sysbrowse


def set_mtime_ns(path: Path, mtime_ns: int) -> None:
    os.utime(path, ns=(mtime_ns, mtime_ns))


def main() -> int:
    original_scan = sysbrowse.scan_file_with_sysmvp
    try:
        with tempfile.TemporaryDirectory() as tmp:
            repo_dir = Path(tmp) / "repo"
            watched_dir = repo_dir / "watched"
            watched_dir.mkdir(parents=True)
            (repo_dir / ".sysignore").write_text(".sysstore/\n.sysmvp.db\n", encoding="utf-8")

            scan_calls: list[Path] = []

            def fake_scan(root: Path, file_path: Path) -> sysbrowse.ActionMessage:
                assert root == repo_dir
                scan_calls.append(file_path.resolve())
                return sysbrowse.ActionMessage("success", "Scanned", "")

            sysbrowse.scan_file_with_sysmvp = fake_scan

            stable_file = watched_dir / "stable.txt"
            stable_file.write_text("alpha\n", encoding="utf-8")
            set_mtime_ns(stable_file, 1_700_000_000_000_000_000)

            stable_handle = sysbrowse.RootWatchHandle(repo_dir, "watched")
            assert stable_handle.record_path_change(stable_file) is True
            stable_pending = stable_handle._pending[str(stable_file.resolve())]
            stable_handle.process_due_files_once(now=stable_pending.last_event_monotonic + 61)
            assert scan_calls == [stable_file.resolve()]
            assert stable_handle._pending == {}

            bouncing_file = watched_dir / "bouncing.txt"
            bouncing_file.write_text("first\n", encoding="utf-8")
            set_mtime_ns(bouncing_file, 1_700_000_010_000_000_000)

            bouncing_handle = sysbrowse.RootWatchHandle(repo_dir, "watched")
            assert bouncing_handle.record_path_change(bouncing_file) is True
            first_pending = bouncing_handle._pending[str(bouncing_file.resolve())]

            bouncing_file.write_text("second\n", encoding="utf-8")
            set_mtime_ns(bouncing_file, 1_700_000_020_000_000_000)

            bouncing_handle.process_due_files_once(now=first_pending.last_event_monotonic + 61)
            assert scan_calls == [stable_file.resolve()]
            assert str(bouncing_file.resolve()) in bouncing_handle._pending

            second_pending = bouncing_handle._pending[str(bouncing_file.resolve())]
            assert second_pending.observed_mtime_ns == 1_700_000_020_000_000_000

            bouncing_handle.process_due_files_once(now=second_pending.last_event_monotonic + 61)
            assert scan_calls == [stable_file.resolve(), bouncing_file.resolve()]
            assert bouncing_handle._pending == {}
    finally:
        sysbrowse.scan_file_with_sysmvp = original_scan

    print("[root-watch-smoke] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
