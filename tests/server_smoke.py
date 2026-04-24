#!/usr/bin/env python3
import hashlib
import importlib.util
import os
import sqlite3
import shutil
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent


def set_mtime(path: Path, timestamp: int) -> None:
    os.utime(path, (timestamp, timestamp))


def wait_for_server_url(handle: subprocess.Popen[str]) -> str:
    deadline = time.time() + 10
    while time.time() < deadline:
        line = handle.stderr.readline()
        if "http://" in line:
            return line.strip().rsplit(" ", 1)[-1]
    raise RuntimeError("server did not report a listening URL")


def http_get_text(url: str) -> str:
    with urllib.request.urlopen(url, timeout=5) as response:
        return response.read().decode("utf-8")


def http_get_text_with_headers(url: str) -> tuple[str, dict[str, str]]:
    with urllib.request.urlopen(url, timeout=5) as response:
        return response.read().decode("utf-8"), dict(response.headers.items())


def http_post_form_text_with_headers(url: str, form: dict[str, str]) -> tuple[str, dict[str, str]]:
    payload = urllib.parse.urlencode(form).encode("utf-8")
    request = urllib.request.Request(url, data=payload, method="POST")
    request.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(request, timeout=5) as response:
        return response.read().decode("utf-8"), dict(response.headers.items())


def http_get_bytes(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=5) as response:
        return response.read()


def run(cmd: list[str], cwd: Path) -> None:
    subprocess.run(cmd, cwd=cwd, check=True, stdout=subprocess.DEVNULL)


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_root = Path(tmp)
        repo_dir = tmp_root / "repo"
        repo_dir.mkdir()

        shutil.copy2(ROOT_DIR / "sysmvp.py", repo_dir / "sysmvp.py")
        shutil.copy2(ROOT_DIR / "schema.sql", repo_dir / "schema.sql")
        shutil.copy2(ROOT_DIR / ".sysignore", repo_dir / ".sysignore")

        subprocess.run(["python3", "sysmvp.py", "init"], cwd=repo_dir, check=True)
        demo_dir = repo_dir / "demo"
        demo_dir.mkdir()
        alpha_path = demo_dir / "a.txt"
        beta_path = demo_dir / "b.txt"
        alpha_path.write_text("alpha\n", encoding="utf-8")
        beta_path.write_text("beta\n", encoding="utf-8")
        set_mtime(alpha_path, 1_700_000_000)
        set_mtime(beta_path, 1_700_000_100)
        subprocess.run(["python3", "sysmvp.py", "scan", "--root", "demo"], cwd=repo_dir, check=True)
        dupes_dir = repo_dir / "dupes"
        dupes_dir.mkdir()
        alpha_copy_path = dupes_dir / "a-copy.txt"
        alpha_copy_path.write_text("alpha\n", encoding="utf-8")
        subprocess.run(["python3", "sysmvp.py", "scan", "--root", "dupes"], cwd=repo_dir, check=True)
        moved_dir = repo_dir / "moved"
        moved_dir.mkdir()
        alpha_moved_path = moved_dir / "alpha-old.txt"
        alpha_moved_path.write_text("alpha\n", encoding="utf-8")
        subprocess.run(["python3", "sysmvp.py", "scan", "--root", "moved"], cwd=repo_dir, check=True)
        empty_dupes_dir = repo_dir / "empty_dupes"
        empty_dupes_dir.mkdir()
        empty_a_path = empty_dupes_dir / "a.txt"
        empty_b_path = empty_dupes_dir / "b.txt"
        empty_a_path.write_text("", encoding="utf-8")
        empty_b_path.write_text("", encoding="utf-8")
        subprocess.run(["python3", "sysmvp.py", "scan", "--root", "empty_dupes"], cwd=repo_dir, check=True)
        other_dir = repo_dir / "other"
        other_dir.mkdir()
        gamma_path = other_dir / "c.txt"
        gamma_path.write_text("gamma\n", encoding="utf-8")
        subprocess.run(["python3", "sysmvp.py", "scan", "--root", "other"], cwd=repo_dir, check=True)
        external_scan_dir = tmp_root / "external_scan"
        external_scan_dir.mkdir()
        external_file_path = external_scan_dir / "external.txt"
        external_file_path.write_text("external\n", encoding="utf-8")
        external_scan_root = external_scan_dir.resolve().as_posix()
        external_file_display_path = external_file_path.resolve().as_posix()
        subprocess.run(["python3", "sysmvp.py", "scan", "--root", external_scan_root], cwd=repo_dir, check=True)
        forget_dir = repo_dir / "forget_me"
        forget_dir.mkdir()
        forgotten_path = forget_dir / "gone.txt"
        forgotten_path.write_text("forget this\n", encoding="utf-8")
        forgotten_hash = hashlib.sha256(forgotten_path.read_bytes()).hexdigest()
        forgotten_blob_path = repo_dir / ".sysstore" / "objects" / forgotten_hash[:2] / forgotten_hash
        subprocess.run(["python3", "sysmvp.py", "scan", "--root", "forget_me"], cwd=repo_dir, check=True)
        empty_scan_dir = repo_dir / "empty_scan"
        empty_scan_dir.mkdir()
        subprocess.run(["python3", "sysmvp.py", "scan", "--root", "empty_scan"], cwd=repo_dir, check=True)
        subprocess.run(["python3", "sysmvp.py", "forget-root", "forget_me"], cwd=repo_dir, check=True)
        subprocess.run(["python3", "sysmvp.py", "forget-root", "empty_scan"], cwd=repo_dir, check=True)
        old_gamma_hash = hashlib.sha256(gamma_path.read_bytes()).hexdigest()
        gamma_path.write_text("gamma updated\n", encoding="utf-8")
        subprocess.run(["python3", "sysmvp.py", "scan", "--root", "other"], cwd=repo_dir, check=True)

        git_repo_dir = repo_dir / "repo_git"
        git_repo_dir.mkdir()
        run(["git", "init", "-b", "main"], git_repo_dir)
        run(["git", "config", "user.name", "SCUM Test"], git_repo_dir)
        run(["git", "config", "user.email", "scum@example.com"], git_repo_dir)

        shared_path = git_repo_dir / "shared.txt"
        shared_path.write_text("shared\n", encoding="utf-8")
        run(["git", "add", "shared.txt"], git_repo_dir)
        run(["git", "commit", "-m", "initial"], git_repo_dir)

        run(["git", "checkout", "-b", "feature"], git_repo_dir)
        feature_only_path = git_repo_dir / "feature.txt"
        feature_only_path.write_text("feature base\n", encoding="utf-8")
        run(["git", "add", "feature.txt"], git_repo_dir)
        run(["git", "commit", "-m", "feature file"], git_repo_dir)

        run(["git", "checkout", "main"], git_repo_dir)
        main_only_path = git_repo_dir / "main.txt"
        main_only_path.write_text("main branch\n", encoding="utf-8")
        run(["git", "add", "main.txt"], git_repo_dir)
        run(["git", "commit", "-m", "main file"], git_repo_dir)

        main_untracked_path = git_repo_dir / "main-untracked.txt"
        main_untracked_path.write_text("scratch\n", encoding="utf-8")
        subprocess.run(["python3", "sysmvp.py", "scan", "--root", "repo_git"], cwd=repo_dir, check=True)

        main_untracked_path.unlink()
        run(["git", "checkout", "feature"], git_repo_dir)
        feature_only_path.write_text("feature changed\n", encoding="utf-8")
        subprocess.run(["python3", "sysmvp.py", "scan", "--root", "repo_git"], cwd=repo_dir, check=True)

        parent_repo_dir = repo_dir / "parent_git"
        parent_repo_dir.mkdir()
        run(["git", "init", "-b", "main"], parent_repo_dir)
        run(["git", "config", "user.name", "SCUM Test"], parent_repo_dir)
        run(["git", "config", "user.email", "scum@example.com"], parent_repo_dir)
        parent_root_file = parent_repo_dir / "tracked.txt"
        parent_root_file.write_text("parent root\n", encoding="utf-8")
        run(["git", "add", "tracked.txt"], parent_repo_dir)
        run(["git", "commit", "-m", "parent root"], parent_repo_dir)
        inherited_scan_dir = parent_repo_dir / "inside_scan"
        inherited_scan_dir.mkdir()
        inherited_file = inherited_scan_dir / "outside-root.txt"
        inherited_file.write_text("should not inherit parent repo\n", encoding="utf-8")
        subprocess.run(["python3", "sysmvp.py", "scan", "--root", "parent_git/inside_scan"], cwd=repo_dir, check=True)

        multi_git_dir = repo_dir / "multi_git"
        multi_git_dir.mkdir()
        run(["git", "init", "-b", "main"], multi_git_dir)
        run(["git", "config", "user.name", "SCUM Test"], multi_git_dir)
        run(["git", "config", "user.email", "scum@example.com"], multi_git_dir)
        outer_git_file = multi_git_dir / "outer.txt"
        outer_git_file.write_text("outer tracked\n", encoding="utf-8")
        run(["git", "add", "outer.txt"], multi_git_dir)
        run(["git", "commit", "-m", "outer file"], multi_git_dir)

        nested_repo_dir = multi_git_dir / "nested_repo"
        nested_repo_dir.mkdir()
        run(["git", "init", "-b", "dev"], nested_repo_dir)
        run(["git", "config", "user.name", "SCUM Test"], nested_repo_dir)
        run(["git", "config", "user.email", "scum@example.com"], nested_repo_dir)
        nested_git_file = nested_repo_dir / "nested.txt"
        nested_git_file.write_text("nested tracked\n", encoding="utf-8")
        run(["git", "add", "nested.txt"], nested_repo_dir)
        run(["git", "commit", "-m", "nested file"], nested_repo_dir)
        subprocess.run(["python3", "sysmvp.py", "scan", "--root", "multi_git"], cwd=repo_dir, check=True)

        conn = sqlite3.connect(repo_dir / ".sysmvp.db")
        conn.row_factory = sqlite3.Row
        try:
            git_branches = {
                str(row["git_branch"])
                for row in conn.execute(
                    """
                    SELECT DISTINCT git_branch
                    FROM file_scan_git
                    WHERE COALESCE(git_repo_root, '') = 'repo_git'
                      AND git_branch IS NOT NULL
                    """
                )
            }
            assert {"main", "feature"} <= git_branches

            main_untracked_state = conn.execute(
                """
                SELECT git_state
                FROM file_scan_git fsg
                WHERE fsg.git_branch = 'main'
                  AND COALESCE(fsg.git_repo_root, '') = 'repo_git'
                  AND fsg.repo_rel_path = 'main-untracked.txt'
                ORDER BY fsg.scan_id DESC
                LIMIT 1
                """
            ).fetchone()
            assert main_untracked_state is not None
            assert main_untracked_state["git_state"] == "untracked"

            feature_modified_state = conn.execute(
                """
                SELECT git_state
                FROM file_scan_git fsg
                WHERE fsg.git_branch = 'feature'
                  AND COALESCE(fsg.git_repo_root, '') = 'repo_git'
                  AND fsg.repo_rel_path = 'feature.txt'
                ORDER BY fsg.scan_id DESC
                LIMIT 1
                """
            ).fetchone()
            assert feature_modified_state is not None
            assert feature_modified_state["git_state"] == "modified"

            inherited_scan_state = conn.execute(
                """
                SELECT COUNT(*)
                FROM file_scan_git fsg
                JOIN file_entry fe ON fe.file_id = fsg.file_id
                WHERE fe.current_path = 'parent_git/inside_scan/outside-root.txt'
                """
            ).fetchone()
            assert inherited_scan_state is not None
            assert int(inherited_scan_state[0]) == 0

            inherited_scan_run = conn.execute(
                """
                SELECT is_git_repo
                FROM scan_run
                WHERE scan_root = 'parent_git/inside_scan'
                ORDER BY scan_id DESC
                LIMIT 1
                """
            ).fetchone()
            assert inherited_scan_run is not None
            assert int(inherited_scan_run["is_git_repo"]) == 0

            nested_repo_state = conn.execute(
                """
                SELECT fsg.git_repo_root, fsg.git_branch
                FROM file_scan_git fsg
                JOIN file_entry fe ON fe.file_id = fsg.file_id
                WHERE fe.current_path = 'multi_git/nested_repo/nested.txt'
                ORDER BY fsg.scan_id DESC
                LIMIT 1
                """
            ).fetchone()
            assert nested_repo_state is not None
            assert nested_repo_state["git_repo_root"] == "multi_git/nested_repo"
            assert nested_repo_state["git_branch"] == "dev"

            outer_repo_state = conn.execute(
                """
                SELECT fsg.git_repo_root, fsg.git_branch
                FROM file_scan_git fsg
                JOIN file_entry fe ON fe.file_id = fsg.file_id
                WHERE fe.current_path = 'multi_git/outer.txt'
                ORDER BY fsg.scan_id DESC
                LIMIT 1
                """
            ).fetchone()
            assert outer_repo_state is not None
            assert outer_repo_state["git_repo_root"] == "multi_git"
            assert outer_repo_state["git_branch"] == "main"

            forgotten_file = conn.execute(
                """
                SELECT 1
                FROM file_entry
                WHERE current_path = 'forget_me/gone.txt'
                """
            ).fetchone()
            assert forgotten_file is None

            forgotten_scan = conn.execute(
                """
                SELECT 1
                FROM scan_run
                WHERE scan_root = 'forget_me'
                """
            ).fetchone()
            assert forgotten_scan is None

            forgotten_empty_scan = conn.execute(
                """
                SELECT 1
                FROM scan_run
                WHERE scan_root = 'empty_scan'
                """
            ).fetchone()
            assert forgotten_empty_scan is None

            external_file = conn.execute(
                """
                SELECT 1
                FROM file_entry
                WHERE current_path = ?
                """,
                (external_file_display_path,),
            ).fetchone()
            assert external_file is not None

            external_scan = conn.execute(
                """
                SELECT 1
                FROM scan_run
                WHERE scan_root = ?
                """,
                (external_scan_root,),
            ).fetchone()
            assert external_scan is not None

            forgotten_blob = conn.execute(
                """
                SELECT 1
                FROM blob_object
                WHERE blob_hash = ?
                """,
                (forgotten_hash,),
            ).fetchone()
            assert forgotten_blob is None
        finally:
            conn.close()

        assert not forgotten_blob_path.exists()

        conn = sqlite3.connect(repo_dir / ".sysmvp.db")
        conn.row_factory = sqlite3.Row
        try:
            beta_file_id = int(
                conn.execute(
                    """
                    SELECT file_id
                    FROM file_entry
                    WHERE current_path = 'demo/b.txt'
                    """
                ).fetchone()[0]
            )
            gamma_file_id = int(
                conn.execute(
                    """
                    SELECT file_id
                    FROM file_entry
                    WHERE current_path = 'other/c.txt'
                    """
                ).fetchone()[0]
            )
            empty_a_file_id = int(
                conn.execute(
                    """
                    SELECT file_id
                    FROM file_entry
                    WHERE current_path = 'empty_dupes/a.txt'
                    """
                ).fetchone()[0]
            )
        finally:
            conn.close()

        blob_hash = hashlib.sha256(alpha_path.read_bytes()).hexdigest()
        gamma_hash = hashlib.sha256(gamma_path.read_bytes()).hexdigest()
        server = subprocess.Popen(
            ["python3", str(ROOT_DIR / "sysbrowse.py"), "--repo", str(repo_dir), "--port", "0"],
            cwd=ROOT_DIR,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            base_url = wait_for_server_url(server)
            index_html = http_get_text(base_url + "/?path=demo")
            assert "SCUM Browser" in index_html
            assert "Stats scope: <span class=\"mono\">Scoped to demo</span>" in index_html
            assert "<div class=\"stat\"><span>Files</span><strong>2</strong></div>" in index_html
            assert "<span>Duplicate Files</span><strong>1</strong>" in index_html
            assert "<div class=\"stat\"><span>Blobs</span><strong>2</strong></div>" in index_html
            assert "<div class=\"stat\"><span>Transactions</span><strong>2</strong></div>" in index_html
            assert "<strong>demo</strong>" in index_html
            assert "Scan Root" in index_html
            assert 'class="active"' in index_html
            assert ">Roots</button>" in index_html
            assert ">Repos</button>" in index_html
            assert ">Files</button>" in index_html
            assert index_html.index(">Roots</button>") < index_html.index(">Repos</button>") < index_html.index(">Files</button>")
            assert 'value="demo"' in index_html
            assert 'list="path-suggestions"' in index_html
            assert 'hx-get="/partials/path-suggestions"' in index_html
            assert "other/c.txt" not in index_html

            files_partial_html, files_partial_headers = http_get_text_with_headers(base_url + "/partials/files?path=demo")
            assert 'id="stats-panel"' in files_partial_html
            assert 'hx-swap-oob="outerHTML"' in files_partial_html
            assert "Scoped to demo" in files_partial_html
            assert "<div class=\"stat\"><span>Files</span><strong>2</strong></div>" in files_partial_html
            assert "<span>Duplicate Files</span><strong>1</strong>" in files_partial_html
            assert files_partial_headers["HX-Push-Url"] == "/?view=files&path=demo"
            assert files_partial_html.index("<strong>demo/b.txt</strong>") < files_partial_html.index("<strong>demo/a.txt</strong>")
            assert 'localStorage.setItem(storageKey, element.open ? "open" : "closed")' in index_html
            assert 'querySelectorAll("details[data-pref-key]")' in index_html

            suggestion_html = http_get_text(base_url + "/partials/path-suggestions?path=de")
            assert '<option value="demo"></option>' in suggestion_html
            assert '<option value="demo/a.txt"></option>' in suggestion_html
            assert '<option value="other"></option>' not in suggestion_html

            detail_html = http_get_text(base_url + "/partials/files/1")
            assert "Fact History" in detail_html
            assert "Blob Preview" in detail_html
            assert "Matching Hashes" in detail_html
            assert "Find matching hashes" in detail_html
            assert "alpha" in detail_html
            assert blob_hash in detail_html
            assert "<span class=\"label-closed\">Show Details</span><span class=\"label-open\">Hide Details</span>" in detail_html
            assert detail_html.index("<td>Modified</td>") < detail_html.index("<summary><span class=\"label-closed\">Show Details</span>")
            assert detail_html.index("<summary><span class=\"label-closed\">Show Details</span>") < detail_html.index("<td>File ID</td>")
            assert 'data-pref-key="blob-preview-visible"' in detail_html
            assert "<span class=\"label-closed\">Show Blob Preview</span><span class=\"label-open\">Hide Blob Preview</span>" in detail_html

            matching_hashes_html = http_get_text(base_url + "/partials/files/1/matching-hashes")
            assert "Showing every transaction where this blob hash was observed." in matching_hashes_html
            assert "demo/a.txt" in matching_hashes_html
            assert "dupes/a-copy.txt" in matching_hashes_html
            assert "moved/alpha-old.txt" in matching_hashes_html
            assert "current file" in matching_hashes_html

            duplicates_html, duplicates_headers = http_get_text_with_headers(base_url + "/partials/duplicates?path=demo")
            assert "Show Duplicates" in duplicates_html
            assert "Returned 1 duplicate hash group." in duplicates_html
            assert "demo/a.txt" in duplicates_html
            assert "dupes/a-copy.txt" not in duplicates_html
            assert "moved/alpha-old.txt" not in duplicates_html
            assert f">{blob_hash}</a>" not in duplicates_html
            assert f">{blob_hash}</td>" in duplicates_html
            assert "<td>3</td>" in duplicates_html
            assert duplicates_headers["HX-Push-Url"] == "/?view=duplicates&path=demo"

            duplicate_search_html = http_get_text(base_url + "/partials/duplicates?q=moved")
            assert "Returned 1 duplicate hash group." in duplicate_search_html
            assert "moved/alpha-old.txt" in duplicate_search_html

            root_duplicates_html = http_get_text(base_url + "/partials/duplicates")
            assert "Returned 1 duplicate hash group." in root_duplicates_html
            assert root_duplicates_html.count("<tr>") == 2

            empty_scope_html = http_get_text(base_url + "/?path=empty_dupes")
            assert "Stats scope: <span class=\"mono\">Scoped to empty_dupes</span>" in empty_scope_html
            assert "<div class=\"stat\"><span>Files</span><strong>2</strong></div>" in empty_scope_html
            assert "<span>Duplicate Files</span><strong>0</strong>" in empty_scope_html

            empty_duplicates_html = http_get_text(base_url + "/partials/duplicates?path=empty_dupes")
            assert "No current files with shared blob hashes matched this scope." in empty_duplicates_html
            assert "No duplicate files matched this query." in empty_duplicates_html

            empty_matching_hashes_html = http_get_text(base_url + f"/partials/files/{empty_a_file_id}/matching-hashes")
            assert "Empty files are excluded from hash-match reporting." in empty_matching_hashes_html

            selected_detail_html, selected_detail_headers = http_get_text_with_headers(
                base_url + f"/partials/files/{beta_file_id}?path=demo"
            )
            assert "demo/b.txt" in selected_detail_html
            assert selected_detail_headers["HX-Push-Url"] == f"/?view=files&path=demo&file={beta_file_id}"

            bookmarked_file_html = http_get_text(base_url + f"/?view=files&path=demo&file={beta_file_id}")
            assert "demo/b.txt" in bookmarked_file_html
            assert 'class="list-item active"' in bookmarked_file_html

            blobs_html = http_get_text(base_url + "/partials/blobs?path=demo")
            assert blob_hash in blobs_html
            assert gamma_hash not in blobs_html

            tx_html = http_get_text(base_url + "/partials/tx?path=demo")
            assert "scan demo/a.txt" in tx_html
            assert "scan other/c.txt" not in tx_html

            version_detail_html = http_get_text(base_url + f"/partials/files/{gamma_file_id}")
            assert "Version History" in version_detail_html
            assert old_gamma_hash in version_detail_html
            assert gamma_hash in version_detail_html
            assert f"/blob/{old_gamma_hash}" in version_detail_html
            assert "/partials/blob-preview/" in version_detail_html
            assert "/partials/blob-diff?" in version_detail_html
            assert "diff prev" in version_detail_html

            old_preview_html = http_get_text(
                base_url + f"/partials/blob-preview/{old_gamma_hash}?kind=text&mime=text/plain"
            )
            assert "gamma" in old_preview_html
            assert "updated" not in old_preview_html
            assert '<pre class="code-block">' in old_preview_html

            diff_html = http_get_text(
                base_url
                + f"/partials/blob-diff?newer={gamma_hash}&older={old_gamma_hash}"
                + "&newer_kind=text&newer_mime=text/plain"
                + "&older_kind=text&older_mime=text/plain"
                + "&newer_label=current&older_label=previous"
            )
            assert "Blob Diff" in diff_html
            assert "previous -> current" in diff_html
            assert "-gamma" in diff_html
            assert "+gamma updated" in diff_html
            assert '<pre class="code-block">' in diff_html

            blob_bytes = http_get_bytes(base_url + f"/blob/{blob_hash}")
            assert blob_bytes == b"alpha\n"

            git_index_html = http_get_text(base_url + "/?path=repo_git")
            assert 'name="branch"' in git_index_html
            assert '<option value="feature"' in git_index_html
            assert '<option value="main"' in git_index_html
            assert "Repo: <span class=\"mono\">repo_git</span>" in git_index_html

            multi_git_html = http_get_text(base_url + "/?path=multi_git")
            assert "Repos Scanned</span><strong>2</strong>" in multi_git_html

            main_branch_html = http_get_text(base_url + "/partials/files?path=repo_git&branch=main")
            assert "<strong>repo_git/main.txt</strong>" in main_branch_html
            assert "<strong>repo_git/feature.txt</strong>" not in main_branch_html
            assert "untracked" in main_branch_html

            feature_branch_html = http_get_text(base_url + "/partials/files?path=repo_git&branch=feature")
            assert "<strong>repo_git/feature.txt</strong>" in feature_branch_html
            assert "<strong>repo_git/main.txt</strong>" not in feature_branch_html
            assert "modified" in feature_branch_html

            feature_modified_html, feature_modified_headers = http_get_text_with_headers(
                base_url + "/partials/files?path=repo_git&branch=feature&git_state=modified"
            )
            assert 'name="git_state"' in feature_modified_html
            assert '<option value="modified" selected="selected">Modified</option>' in feature_modified_html
            assert "<strong>repo_git/feature.txt</strong>" in feature_modified_html
            assert "<strong>repo_git/shared.txt</strong>" not in feature_modified_html
            assert "Scoped to repo_git @ feature [modified]" in feature_modified_html
            assert feature_modified_headers["HX-Push-Url"] == "/?view=files&path=repo_git&branch=feature&git_state=modified"

            repos_html, repos_headers = http_get_text_with_headers(base_url + "/partials/repos?path=multi_git")
            assert "multi_git" in repos_html
            assert "multi_git/nested_repo" in repos_html
            assert "repo_git" not in repos_html
            assert 'hx-get="/partials/files?path=multi_git"' in repos_html
            assert 'hx-get="/partials/files?path=multi_git/nested_repo"' in repos_html
            assert repos_headers["HX-Push-Url"] == "/?view=repos&path=multi_git"

            nested_repo_html = http_get_text(base_url + "/?view=repos&path=multi_git/nested_repo")
            assert "Repo: <span class=\"mono\">multi_git/nested_repo</span>" in nested_repo_html
            assert '<option value="dev"' in nested_repo_html
            assert '<option value="main"' not in nested_repo_html

            roots_html, roots_headers = http_get_text_with_headers(base_url + "/partials/roots?path=parent_git")
            assert "parent_git/inside_scan" in roots_html
            assert "repo_git" not in roots_html
            assert 'hx-get="/partials/files?path=parent_git/inside_scan"' in roots_html
            assert 'hx-post="/actions/root"' in roots_html
            assert 'hx-post="/actions/root-watch"' in roots_html
            assert '<input type="checkbox" name="enabled" value="1"' in roots_html
            assert '>Scan</button>' in roots_html
            assert '>Forget</button>' in roots_html
            assert roots_headers["HX-Push-Url"] == "/?view=roots&path=parent_git"

            if importlib.util.find_spec("watchdog") is None:
                watch_action_html, watch_action_headers = http_post_form_text_with_headers(
                    base_url + "/actions/root-watch",
                    {"root": "demo", "enabled": "1", "path": "demo"},
                )
                assert "Could not watch demo" in watch_action_html
                assert "Install it with `python3 -m pip install watchdog` to enable the checkbox." in watch_action_html
                assert watch_action_headers["HX-Push-Url"] == "/?view=roots&path=demo"

            root_scan_html = http_get_text(base_url + "/?view=roots&path=demo")
            assert "Scan Root" in root_scan_html
            assert "<strong>demo</strong>" in root_scan_html
            assert "<strong>repo_git</strong>" not in root_scan_html

            external_roots_html = http_get_text(
                base_url + "/partials/roots?" + urllib.parse.urlencode({"path": external_scan_root})
            )
            assert external_scan_root in external_roots_html

            external_files_html = http_get_text(
                base_url + "/partials/files?" + urllib.parse.urlencode({"path": external_scan_root})
            )
            assert f"<strong>{external_file_display_path}</strong>" in external_files_html

            demo_new_path = demo_dir / "c.txt"
            demo_new_path.write_text("gamma demo\n", encoding="utf-8")
            set_mtime(demo_new_path, 1_700_000_200)
            scan_action_html, scan_action_headers = http_post_form_text_with_headers(
                base_url + "/actions/root",
                {"action": "scan", "root": "demo", "path": "demo"},
            )
            assert "Scanned demo" in scan_action_html
            assert "<div class=\"stat\"><span>Files</span><strong>3</strong></div>" in scan_action_html
            assert scan_action_headers["HX-Push-Url"] == "/?view=roots&path=demo"

            rescanned_files_html = http_get_text(base_url + "/partials/files?path=demo")
            assert "<strong>demo/c.txt</strong>" in rescanned_files_html
            assert rescanned_files_html.index("<strong>demo/c.txt</strong>") < rescanned_files_html.index("<strong>demo/b.txt</strong>")

            forget_action_html, forget_action_headers = http_post_form_text_with_headers(
                base_url + "/actions/root",
                {"action": "forget", "root": "moved", "path": "moved"},
            )
            assert "Forgot moved" in forget_action_html
            assert "No scanned non-repo roots matched this scope." in forget_action_html
            assert forget_action_headers["HX-Push-Url"] == "/?view=roots&path=moved"

            external_forget_html, external_forget_headers = http_post_form_text_with_headers(
                base_url + "/actions/root",
                {"action": "forget", "root": external_scan_root, "path": external_scan_root},
            )
            assert f"Forgot {external_scan_root}" in external_forget_html
            assert "No scanned non-repo roots matched this scope." in external_forget_html
            assert external_forget_headers["HX-Push-Url"] == "/?view=roots&path=" + urllib.parse.quote(
                external_scan_root,
                safe="",
            )

            conn = sqlite3.connect(repo_dir / ".sysmvp.db")
            conn.row_factory = sqlite3.Row
            try:
                forgotten_external_file = conn.execute(
                    """
                    SELECT 1
                    FROM file_entry
                    WHERE current_path = ?
                    """,
                    (external_file_display_path,),
                ).fetchone()
                assert forgotten_external_file is None

                forgotten_external_scan = conn.execute(
                    """
                    SELECT 1
                    FROM scan_run
                    WHERE scan_root = ?
                    """,
                    (external_scan_root,),
                ).fetchone()
                assert forgotten_external_scan is None
            finally:
                conn.close()

            sql_query = urllib.parse.quote(
                "SELECT current_path FROM file_entry WHERE current_path LIKE 'demo/%' ORDER BY current_path"
            )
            sql_html, sql_headers = http_get_text_with_headers(base_url + f"/partials/sql?sql={sql_query}&path=demo")
            assert "Run Query" in sql_html
            assert "Returned 3 rows." in sql_html
            assert "demo/a.txt" in sql_html
            assert "demo/b.txt" in sql_html
            assert "demo/c.txt" in sql_html
            assert "other/c.txt" not in sql_html
            assert 'href="/?view=files&amp;path=demo&amp;file=1"' in sql_html
            assert 'href="/?view=files&amp;path=demo&amp;file=2"' in sql_html
            assert sql_headers["HX-Push-Url"] == "/?" + urllib.parse.urlencode(
                {
                    "view": "sql",
                    "path": "demo",
                    "sql": "SELECT current_path FROM file_entry WHERE current_path LIKE 'demo/%' ORDER BY current_path",
                }
            )

            sql_index_html = http_get_text(base_url + "/?view=sql")
            assert "Only <span class=\"mono\">SELECT</span> statements are allowed." in sql_index_html

            rejected_sql = urllib.parse.quote("DELETE FROM file_entry")
            rejected_sql_html = http_get_text(base_url + f"/partials/sql?sql={rejected_sql}")
            assert "Query rejected" in rejected_sql_html
            assert "Only SELECT queries are allowed." in rejected_sql_html
        finally:
            server.terminate()
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=5)

    print("[server-smoke] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
