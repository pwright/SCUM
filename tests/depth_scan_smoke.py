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

        scan_root = repo_dir / "examples" / "demo"
        level1 = scan_root / "level1"
        level2 = level1 / "level2"
        level3 = level2 / "level3"
        level3.mkdir(parents=True)
        (scan_root / "root.txt").write_text("root\n", encoding="utf-8")
        (level1 / "one.txt").write_text("one\n", encoding="utf-8")
        (level2 / "two.txt").write_text("two\n", encoding="utf-8")
        (level3 / "three.txt").write_text("three\n", encoding="utf-8")

        run_sysmvp(repo_dir, "scan", "--depth", "2", "examples/demo")
        assert listed_paths(repo_dir) == [
            "examples/demo/root.txt",
            "examples/demo/level1/one.txt",
            "examples/demo/level1/level2/two.txt",
        ]

        shallow_root = repo_dir / "examples" / "shallow"
        shallow_child = shallow_root / "child"
        shallow_child.mkdir(parents=True)
        (shallow_root / "root.txt").write_text("shallow root\n", encoding="utf-8")
        (shallow_child / "child.txt").write_text("shallow child\n", encoding="utf-8")

        run_sysmvp(repo_dir, "scan", "--root", "examples/shallow", "--depth", "0")
        assert listed_paths(repo_dir) == [
            "examples/demo/root.txt",
            "examples/demo/level1/one.txt",
            "examples/demo/level1/level2/two.txt",
            "examples/shallow/root.txt",
        ]

    print("[depth-scan-smoke] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
