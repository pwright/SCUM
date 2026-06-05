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


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        repo_dir = Path(tmp) / "repo"
        repo_dir.mkdir()

        shutil.copy2(ROOT_DIR / "sysmvp.py", repo_dir / "sysmvp.py")
        shutil.copy2(ROOT_DIR / "schema.sql", repo_dir / "schema.sql")
        shutil.copy2(ROOT_DIR / ".sysignore", repo_dir / ".sysignore")

        run_sysmvp(repo_dir, "init")

        demo = repo_dir / "demo"
        docs = demo / "docs" / "api"
        docs.mkdir(parents=True)
        (demo / "a.txt").write_text("alpha\n", encoding="utf-8")
        (docs / "b.txt").write_text("beta\n", encoding="utf-8")
        run_sysmvp(repo_dir, "scan", "--root", "demo")

        sidebar = sysbrowse.render_tree_sidebar(repo_dir, "demo/docs")
        assert 'id="tree-sidebar"' in sidebar
        assert '<h2 class="tree-title">Roots</h2>' in sidebar
        assert 'hx-get="/partials/files?path=demo"' in sidebar
        assert 'hx-get="/partials/files?path=demo/docs"' in sidebar
        assert 'hx-get="/partials/files?path=demo/docs/api"' in sidebar
        assert 'data-pref-key="tree:demo"' in sidebar
        assert 'data-pref-key="tree:demo/docs"' in sidebar
        assert '<span class="tree-caret">›</span>' in sidebar
        assert 'class="tree-link tree-select active"' in sidebar
        assert '<span class="tree-label">api</span>' in sidebar

        partial = sysbrowse.render_partial_response(repo_dir, "demo/docs/api", "", "", "<div>content</div>")
        assert 'id="tree-sidebar"' in partial
        assert 'hx-swap-oob="outerHTML"' in partial
        assert 'value="demo/docs/api"' in partial
        assert 'class="tree-link active"' in partial

    print("[tree-browser-smoke] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
