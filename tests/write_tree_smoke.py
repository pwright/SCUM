#!/usr/bin/env python3
import shutil
import subprocess
import tempfile
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent


def run_sysmvp(repo_dir: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["python3", "sysmvp.py", *args],
        cwd=repo_dir,
        check=check,
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

        source_root = repo_dir / "examples" / "demo"
        level1 = source_root / "level1"
        level2 = level1 / "level2"
        level3 = level2 / "level3"
        level3.mkdir(parents=True)
        (source_root / "root.txt").write_text("root\n", encoding="utf-8")
        (level1 / "one.txt").write_text("one\n", encoding="utf-8")
        (level2 / "two.txt").write_text("two\n", encoding="utf-8")
        (level3 / "three.txt").write_text("three\n", encoding="utf-8")

        run_sysmvp(repo_dir, "scan", "--root", "examples/demo")
        run_sysmvp(repo_dir, "remove", "examples/demo/level1/level2/level3")

        output_dir = Path(tmp) / "out"
        run_sysmvp(repo_dir, "write", "--depth", "2", "examples/demo", "-o", str(output_dir))

        assert (output_dir / "root.txt").read_text(encoding="utf-8") == "root\n"
        assert (output_dir / "level1" / "one.txt").read_text(encoding="utf-8") == "one\n"
        assert (output_dir / "level1" / "level2" / "two.txt").read_text(encoding="utf-8") == "two\n"
        assert not (output_dir / "level1" / "level2" / "level3" / "three.txt").exists()

        blocked = run_sysmvp(repo_dir, "write", "examples/demo", "-o", str(output_dir), check=False)
        assert blocked.returncode != 0
        assert "pass --overwrite" in blocked.stderr

        (output_dir / "root.txt").write_text("changed\n", encoding="utf-8")
        run_sysmvp(repo_dir, "write", "examples/demo/root.txt", "-o", str(output_dir), "--overwrite")
        assert (output_dir / "root.txt").read_text(encoding="utf-8") == "root\n"

        external_root = Path(tmp) / ".ansible"
        external_child = external_root / "roles" / "web"
        external_child.mkdir(parents=True)
        (external_root / "site.yml").write_text("site\n", encoding="utf-8")
        (external_root / "roles" / "main.yml").write_text("role\n", encoding="utf-8")
        (external_child / "tasks.yml").write_text("tasks\n", encoding="utf-8")

        run_sysmvp(repo_dir, "scan", "--root", str(external_root))
        external_out = Path(tmp) / "external-out"
        run_sysmvp(repo_dir, "write", "--depth", "1", str(external_root), "-o", str(external_out))
        assert (external_out / "site.yml").read_text(encoding="utf-8") == "site\n"
        assert (external_out / "roles" / "main.yml").read_text(encoding="utf-8") == "role\n"
        assert not (external_out / "roles" / "web" / "tasks.yml").exists()

    print("[write-tree-smoke] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
