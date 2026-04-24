#!/usr/bin/env python3
import argparse
import difflib
import fnmatch
import html
import json
import mimetypes
import os
import sqlite3
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, quote, urlencode, urlparse

DB_NAME = ".sysmvp.db"
STORE_DIR = ".sysstore/objects"
GIT_STATE_OPTIONS = ("clean", "modified", "added", "renamed", "copied", "untracked")
VIEW_OPTIONS = ("files", "duplicates", "repos", "roots", "blobs", "tx", "sql")
SQL_QUERY_ROW_LIMIT = 200
WATCH_STABILITY_WINDOW_SECONDS = 60.0
SQL_QUERY_DEFAULT = """SELECT
  fe.current_path,
  json_extract(vcf.value_json, '$.header') AS header
FROM file_entry fe
JOIN v_current_fact vcf ON vcf.entity_id = fe.file_id
JOIN attribute a ON a.attr_id = vcf.attr_id
WHERE a.ident = 'asciidoc/header'
ORDER BY fe.current_path
LIMIT 50"""

try:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer
except ImportError as exc:  # pragma: no cover - exercised through runtime fallback
    FileSystemEventHandler = object  # type: ignore[assignment,misc]
    Observer = None  # type: ignore[assignment]
    WATCHDOG_IMPORT_ERROR = str(exc)
else:
    WATCHDOG_IMPORT_ERROR = ""


ROOT_WATCH_MANAGER: Optional["RootWatchManager"] = None


@dataclass(frozen=True)
class RepoContext:
    repo_root: str
    branches: tuple[str, ...]


@dataclass(frozen=True)
class RepoSummary:
    repo_root: str
    branches: tuple[str, ...]
    scans_count: int
    files_count: int
    latest_scan_time: str


@dataclass(frozen=True)
class ScanRootSummary:
    scan_root: str
    scans_count: int
    files_count: int
    latest_scan_time: str


@dataclass(frozen=True)
class ActionMessage:
    level: str
    title: str
    detail: str


@dataclass(frozen=True)
class PendingWatchFile:
    path: Path
    observed_mtime_ns: int
    last_event_monotonic: float


@dataclass(frozen=True)
class RootWatchSummary:
    scan_root: str
    active: bool
    pending_files: int


def repo_root_from(path: Path) -> Path:
    return path.resolve()


def db_path(root: Path) -> Path:
    return root / DB_NAME


def store_root(root: Path) -> Path:
    return root / STORE_DIR


def connect_db(root: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path(root)))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def connect_db_readonly(root: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path(root).resolve()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def ensure_repo_exists(root: Path) -> None:
    if not db_path(root).exists():
        raise SystemExit(f"Repository not initialized at {root}. Run: python3 sysmvp.py init")


def h(value: object) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def fmt_bytes(size: Optional[int]) -> str:
    if size is None:
        return "-"
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{size} B"


def blob_abspath(root: Path, blob_hash: str) -> Path:
    return store_root(root) / blob_hash[:2] / blob_hash


def path_is_within_prefix(path_prefix: str, candidate_prefix: str) -> bool:
    if not path_prefix:
        return False
    if candidate_prefix == "":
        return True
    return path_prefix == candidate_prefix or path_prefix.startswith(candidate_prefix + "/")


def prefix_matches_scope(path_prefix: str, candidate_prefix: str) -> bool:
    if not path_prefix:
        return True
    return (
        path_is_within_prefix(path_prefix, candidate_prefix)
        or candidate_prefix == path_prefix
        or candidate_prefix.startswith(path_prefix + "/")
    )


def path_is_within(path: Path, ancestor: Path) -> bool:
    try:
        path.relative_to(ancestor)
        return True
    except ValueError:
        return False


def resolve_repo_context(root: Path, path_prefix: str) -> Optional[RepoContext]:
    normalized_path = normalize_path_prefix(path_prefix)
    if not normalized_path:
        return None
    conn = connect_db(root)
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT COALESCE(git_repo_root, '') AS git_repo_root
            FROM file_scan_git
            WHERE git_repo_root IS NOT NULL
              AND git_branch IS NOT NULL
            """
        ).fetchall()
        matching_roots = [
            str(row["git_repo_root"])
            for row in rows
            if path_is_within_prefix(normalized_path, str(row["git_repo_root"]))
        ]
        if not matching_roots:
            return None
        repo_root = max(matching_roots, key=len)
        branch_rows = conn.execute(
            """
            SELECT DISTINCT git_branch
            FROM file_scan_git
            WHERE git_repo_root IS NOT NULL
              AND git_branch IS NOT NULL
              AND COALESCE(git_repo_root, '') = ?
            ORDER BY git_branch COLLATE NOCASE, git_branch
            """,
            (repo_root,),
        ).fetchall()
    finally:
        conn.close()
    branches = tuple(str(row["git_branch"]) for row in branch_rows)
    if not branches:
        return None
    return RepoContext(repo_root, branches)


def normalize_branch_name(branch: str) -> str:
    return branch.strip()


def normalize_view_name(view: str) -> str:
    normalized = view.strip().lower()
    if normalized not in VIEW_OPTIONS:
        return "roots"
    return normalized


def normalize_sql_query(query: str) -> str:
    return query.strip()


def normalize_action_root(root_value: str) -> str:
    return normalize_path_prefix(root_value) or "."


def sysmvp_script_path(root: Path) -> Path:
    repo_local = root / "sysmvp.py"
    if repo_local.exists():
        return repo_local
    return Path(__file__).resolve().with_name("sysmvp.py")


def log_browser(message: str) -> None:
    print(f"[sysbrowse] {message}", file=sys.stderr)


def resolve_watch_root_path(root: Path, root_value: str) -> Path:
    raw_value = root_value.strip()
    if not raw_value or raw_value == ".":
        return root.resolve()
    candidate = Path(raw_value)
    if candidate.is_absolute():
        return candidate.resolve()
    return (root / normalize_action_root(raw_value)).resolve()


def load_watch_ignore_patterns(root: Path) -> list[str]:
    ignore_path = root / ".sysignore"
    patterns: list[str] = []
    if not ignore_path.exists():
        return patterns
    for line in ignore_path.read_text(encoding="utf-8").splitlines():
        value = line.strip()
        if not value or value.startswith("#"):
            continue
        patterns.append(value)
    return patterns


def is_ignored_watch_path(root: Path, path: Path, patterns: tuple[str, ...]) -> bool:
    try:
        rel_path = os.path.relpath(path, root)
    except ValueError:
        rel_path = str(path)
    normalized = normalize_path_prefix(rel_path)
    if normalized == DB_NAME or normalized.startswith(STORE_DIR + "/"):
        return True
    parts = normalized.split("/") if normalized else []
    for pattern in patterns:
        candidate = pattern.strip()
        if not candidate:
            continue
        if candidate.endswith("/"):
            dirname = candidate[:-1]
            if dirname and dirname in parts:
                return True
            if normalized.startswith(candidate):
                return True
        if fnmatch.fnmatch(normalized, candidate):
            return True
        if fnmatch.fnmatch(Path(normalized).name, candidate):
            return True
    return False


def scan_file_with_sysmvp(root: Path, file_path: Path) -> ActionMessage:
    script_path = sysmvp_script_path(root)
    command = [sys.executable, str(script_path), "scan", "--file", str(file_path)]
    completed = subprocess.run(
        command,
        cwd=root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    details = [line.strip() for line in (completed.stderr + "\n" + completed.stdout).splitlines() if line.strip()]
    detail = details[-1] if details else ""
    display_path = normalize_path_prefix(os.path.relpath(file_path, root))
    if completed.returncode != 0:
        return ActionMessage(
            "error",
            f"Watch scan failed for {display_path or file_path.name}",
            detail or f"scan exited with code {completed.returncode}",
        )
    return ActionMessage("success", f"Scanned {display_path or file_path.name}", detail or "Command completed.")


class RootWatchEventHandler(FileSystemEventHandler):
    def __init__(self, handle: "RootWatchHandle") -> None:
        self.handle = handle

    def on_created(self, event) -> None:  # pragma: no cover - exercised via watchdog runtime
        if not getattr(event, "is_directory", False):
            self.handle.record_path_change(Path(event.src_path))

    def on_modified(self, event) -> None:  # pragma: no cover - exercised via watchdog runtime
        if not getattr(event, "is_directory", False):
            self.handle.record_path_change(Path(event.src_path))

    def on_moved(self, event) -> None:  # pragma: no cover - exercised via watchdog runtime
        if getattr(event, "is_directory", False):
            return
        destination = getattr(event, "dest_path", "")
        if destination:
            self.handle.record_path_change(Path(destination))


class RootWatchHandle:
    def __init__(
        self,
        repo_root: Path,
        scan_root: str,
        stability_window_seconds: float = WATCH_STABILITY_WINDOW_SECONDS,
    ) -> None:
        self.repo_root = repo_root
        self.scan_root = normalize_action_root(scan_root)
        self.scan_root_path = resolve_watch_root_path(repo_root, scan_root)
        self.stability_window_seconds = stability_window_seconds
        self.ignore_patterns = tuple(load_watch_ignore_patterns(repo_root))
        self._condition = threading.Condition()
        self._pending: dict[str, PendingWatchFile] = {}
        self._observer = None
        self._worker = threading.Thread(
            target=self._run_worker,
            name=f"scum-watch:{self.scan_root or '.'}",
            daemon=True,
        )
        self._stop_requested = False

    def start(self) -> None:
        if Observer is None:
            raise RuntimeError("watchdog is not installed")
        if not self.scan_root_path.exists():
            raise FileNotFoundError(f"Root not found: {self.scan_root_path}")
        if not self.scan_root_path.is_dir():
            raise NotADirectoryError(f"Watch root is not a directory: {self.scan_root_path}")
        self._observer = Observer()
        self._observer.schedule(RootWatchEventHandler(self), str(self.scan_root_path), recursive=True)
        self._observer.start()
        self._worker.start()
        log_browser(f"Watching {self.scan_root or '.'} with a {int(self.stability_window_seconds)}s stability window")

    def stop(self) -> None:
        with self._condition:
            self._stop_requested = True
            self._condition.notify_all()
        if self._observer is not None:
            self._observer.stop()
            self._observer.join(timeout=5)
        if self._worker.is_alive():
            self._worker.join(timeout=5)
        log_browser(f"Stopped watching {self.scan_root or '.'}")

    def summary(self) -> RootWatchSummary:
        with self._condition:
            return RootWatchSummary(
                scan_root=self.scan_root,
                active=self._observer is not None and self._observer.is_alive(),
                pending_files=len(self._pending),
            )

    def record_path_change(self, path: Path) -> bool:
        try:
            resolved = path.resolve()
        except OSError:
            return False
        if not path_is_within(resolved, self.scan_root_path):
            return False
        try:
            stat_result = resolved.stat()
        except OSError:
            return False
        if not resolved.is_file():
            return False
        if is_ignored_watch_path(self.repo_root, resolved, self.ignore_patterns):
            return False
        with self._condition:
            self._pending[str(resolved)] = PendingWatchFile(
                path=resolved,
                observed_mtime_ns=stat_result.st_mtime_ns,
                last_event_monotonic=time.monotonic(),
            )
            self._condition.notify_all()
        return True

    def process_due_files_once(self, now: Optional[float] = None) -> int:
        current_time = time.monotonic() if now is None else now
        due_items: list[PendingWatchFile] = []
        with self._condition:
            for key, item in list(self._pending.items()):
                if item.last_event_monotonic + self.stability_window_seconds <= current_time:
                    due_items.append(item)
                    del self._pending[key]
        processed = 0
        for item in due_items:
            self._process_due_file(item)
            processed += 1
        return processed

    def _run_worker(self) -> None:
        while True:
            with self._condition:
                while not self._stop_requested and not self._pending:
                    self._condition.wait()
                if self._stop_requested:
                    return
                next_ready = min(
                    item.last_event_monotonic + self.stability_window_seconds
                    for item in self._pending.values()
                )
                timeout = max(0.0, next_ready - time.monotonic())
            if timeout > 0:
                with self._condition:
                    if self._stop_requested:
                        return
                    self._condition.wait(timeout)
                    if self._stop_requested:
                        return
            self.process_due_files_once()

    def _process_due_file(self, item: PendingWatchFile) -> None:
        try:
            stat_result = item.path.stat()
        except OSError:
            return
        if not item.path.is_file():
            return
        if is_ignored_watch_path(self.repo_root, item.path, self.ignore_patterns):
            return
        if stat_result.st_mtime_ns != item.observed_mtime_ns:
            with self._condition:
                existing = self._pending.get(str(item.path))
                if existing is None or existing.last_event_monotonic <= item.last_event_monotonic:
                    self._pending[str(item.path)] = PendingWatchFile(
                        path=item.path,
                        observed_mtime_ns=stat_result.st_mtime_ns,
                        last_event_monotonic=time.monotonic(),
                    )
                    self._condition.notify_all()
            return
        result = scan_file_with_sysmvp(self.repo_root, item.path)
        if result.level == "error":
            log_browser(result.detail or result.title)


class RootWatchManager:
    def __init__(self, repo_root: Path, stability_window_seconds: float = WATCH_STABILITY_WINDOW_SECONDS) -> None:
        self.repo_root = repo_root
        self.stability_window_seconds = stability_window_seconds
        self._lock = threading.Lock()
        self._handles: dict[str, RootWatchHandle] = {}

    def availability_detail(self) -> str:
        if Observer is not None:
            return (
                f"Changed files are scanned with `--file` after "
                f"{int(self.stability_window_seconds)} seconds without further edits."
            )
        return (
            "Live watch requires the `watchdog` package. "
            "Install it with `python3 -m pip install watchdog` to enable the checkbox."
        )

    def snapshot(self) -> dict[str, RootWatchSummary]:
        with self._lock:
            return {scan_root: handle.summary() for scan_root, handle in self._handles.items()}

    def set_enabled(self, scan_root: str, enabled: bool) -> ActionMessage:
        normalized_root = normalize_action_root(scan_root)
        if enabled:
            if Observer is None:
                return ActionMessage(
                    "error",
                    f"Could not watch {normalized_root}",
                    self.availability_detail(),
                )
            with self._lock:
                existing = self._handles.get(normalized_root)
                if existing is not None and existing.summary().active:
                    return ActionMessage(
                        "success",
                        f"Watching {normalized_root}",
                        self.availability_detail(),
                    )
            handle = RootWatchHandle(
                repo_root=self.repo_root,
                scan_root=normalized_root,
                stability_window_seconds=self.stability_window_seconds,
            )
            try:
                handle.start()
            except Exception as exc:
                return ActionMessage("error", f"Could not watch {normalized_root}", str(exc))
            with self._lock:
                self._handles[normalized_root] = handle
            return ActionMessage("success", f"Watching {normalized_root}", self.availability_detail())

        with self._lock:
            handle = self._handles.pop(normalized_root, None)
        if handle is not None:
            handle.stop()
        return ActionMessage("success", f"Stopped watching {normalized_root}", "")

    def stop_all(self) -> None:
        with self._lock:
            handles = list(self._handles.values())
            self._handles.clear()
        for handle in handles:
            handle.stop()


def run_sysmvp_action(root: Path, action: str, root_value: str) -> ActionMessage:
    normalized_root = normalize_action_root(root_value)
    script_path = sysmvp_script_path(root)
    if action == "scan":
        command = [sys.executable, str(script_path), "scan", "--root", normalized_root]
        title = f"Scanned {normalized_root}"
    elif action == "forget":
        command = [sys.executable, str(script_path), "forget-root", normalized_root]
        title = f"Forgot {normalized_root}"
    else:
        return ActionMessage("error", "Unknown action", f"Unsupported root action: {action}")

    completed = subprocess.run(
        command,
        cwd=root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    details = [line.strip() for line in (completed.stderr + "\n" + completed.stdout).splitlines() if line.strip()]
    detail = details[-1] if details else ""
    if completed.returncode != 0:
        return ActionMessage("error", f"{title} failed", detail or f"{action} exited with code {completed.returncode}")
    return ActionMessage("success", title, detail or "Command completed.")


def is_select_sql(query: str) -> bool:
    normalized = normalize_sql_query(query).lower()
    return normalized.startswith("select") or normalized.startswith("with")


def resolve_active_branch(root: Path, path_prefix: str, branch: str) -> tuple[Optional[RepoContext], str]:
    repo_ctx = resolve_repo_context(root, path_prefix)
    normalized_branch = normalize_branch_name(branch)
    if repo_ctx is None or normalized_branch not in repo_ctx.branches:
        return repo_ctx, ""
    return repo_ctx, normalized_branch


def resolve_active_git_state(root: Path, path_prefix: str, branch: str, git_state: str) -> str:
    _, active_branch = resolve_active_branch(root, path_prefix, branch)
    if not active_branch:
        return ""
    normalized_state = git_state.strip().lower()
    if normalized_state not in GIT_STATE_OPTIONS:
        return ""
    return normalized_state


def latest_branch_scope_cte(repo_root: str, branch: str) -> tuple[str, tuple[object, ...]]:
    return (
        """
        WITH branch_scope AS (
            SELECT file_id, git_state, git_status_raw, repo_rel_path
            FROM (
                SELECT
                    fsg.file_id,
                    fsg.git_state,
                    fsg.git_status_raw,
                    fsg.repo_rel_path,
                    ROW_NUMBER() OVER (
                        PARTITION BY fsg.file_id
                        ORDER BY sr.scan_time DESC, sr.scan_id DESC
                    ) AS rn
                FROM file_scan_git fsg
                JOIN scan_run sr ON sr.scan_id = fsg.scan_id
                WHERE fsg.git_branch = ?
                  AND COALESCE(fsg.git_repo_root, '') = ?
            )
            WHERE rn = 1
        )
        """,
        (branch, repo_root),
    )


def fetch_stats(root: Path, path_prefix: str, branch: str, git_state: str) -> dict[str, int]:
    normalized_path = normalize_path_prefix(path_prefix).lower()
    path_like = f"{normalized_path}/%" if normalized_path else ""
    repo_ctx, active_branch = resolve_active_branch(root, path_prefix, branch)
    active_git_state = resolve_active_git_state(root, path_prefix, branch, git_state)
    repo_summaries = fetch_repo_summaries(root, path_prefix)
    cte = ""
    cte_params: tuple[object, ...] = ()
    file_join = ""
    blob_join = ""
    tx_join = ""
    duplicate_join = ""
    file_state_filter = ""
    blob_state_filter = ""
    tx_state_filter = ""
    duplicate_state_filter = ""
    if repo_ctx is not None and active_branch:
        cte, cte_params = latest_branch_scope_cte(repo_ctx.repo_root, active_branch)
        file_join = "JOIN branch_scope bs ON bs.file_id = fe.file_id"
        blob_join = "JOIN branch_scope bs ON bs.file_id = fe.file_id"
        tx_join = "JOIN branch_scope bs ON bs.file_id = fe.file_id"
        duplicate_join = "JOIN branch_scope bs_all ON bs_all.file_id = fe_all.file_id"
        if active_git_state:
            file_state_filter = "AND bs.git_state = ?"
            blob_state_filter = "AND bs.git_state = ?"
            tx_state_filter = "AND bs.git_state = ?"
            duplicate_state_filter = "AND bs_all.git_state = ?"
    conn = connect_db(root)
    try:
        row = conn.execute(
            f"""
            {cte}
            SELECT
                (
                    SELECT COUNT(*)
                    FROM file_entry fe
                    {file_join}
                    WHERE fe.is_deleted = 0
                      {file_state_filter}
                      AND (
                            ? = ''
                            OR lower(COALESCE(fe.current_path, fe.canonical_uri)) = ?
                            OR lower(COALESCE(fe.current_path, fe.canonical_uri)) LIKE ?
                      )
                ) AS files_count,
                (
                    SELECT COUNT(DISTINCT bo.blob_hash)
                    FROM blob_object bo
                    JOIN file_entry fe ON fe.current_hash = bo.blob_hash
                    {blob_join}
                    WHERE fe.is_deleted = 0
                      {blob_state_filter}
                      AND (
                            ? = ''
                            OR lower(COALESCE(fe.current_path, fe.canonical_uri)) = ?
                            OR lower(COALESCE(fe.current_path, fe.canonical_uri)) LIKE ?
                      )
                ) AS blobs_count,
                (
                    SELECT COUNT(DISTINCT tx.tx_id)
                    FROM tx
                    JOIN fact f ON f.tx_id = tx.tx_id
                    JOIN file_entry fe ON fe.file_id = f.entity_id
                    {tx_join}
                    WHERE fe.is_deleted = 0
                      {tx_state_filter}
                      AND (
                            ? = ''
                            OR lower(COALESCE(fe.current_path, fe.canonical_uri)) = ?
                            OR lower(COALESCE(fe.current_path, fe.canonical_uri)) LIKE ?
                      )
                ) AS tx_count,
                (
                    SELECT COUNT(*)
                    FROM file_entry fe
                    {file_join}
                    WHERE fe.is_deleted = 0
                      AND fe.current_hash IS NOT NULL
                      AND COALESCE(fe.current_size_bytes, 0) > 0
                      {file_state_filter}
                      AND (
                            ? = ''
                            OR lower(COALESCE(fe.current_path, fe.canonical_uri)) = ?
                            OR lower(COALESCE(fe.current_path, fe.canonical_uri)) LIKE ?
                      )
                      AND fe.current_hash IN (
                            SELECT fe_all.current_hash
                            FROM file_entry fe_all
                            {duplicate_join}
                            WHERE fe_all.is_deleted = 0
                              AND fe_all.current_hash IS NOT NULL
                              AND COALESCE(fe_all.current_size_bytes, 0) > 0
                              {duplicate_state_filter}
                            GROUP BY fe_all.current_hash
                            HAVING COUNT(*) > 1
                      )
                ) AS duplicate_files_count
            """
            ,
            cte_params + (
                *((active_git_state,) if active_git_state else ()),
                normalized_path,
                normalized_path,
                path_like,
                *((active_git_state,) if active_git_state else ()),
                normalized_path,
                normalized_path,
                path_like,
                *((active_git_state,) if active_git_state else ()),
                normalized_path,
                normalized_path,
                path_like,
                *((active_git_state,) if active_git_state else ()),
                normalized_path,
                normalized_path,
                path_like,
                *((active_git_state,) if active_git_state else ()),
            ),
        ).fetchone()
        return {
            "files_count": int(row["files_count"]),
            "blobs_count": int(row["blobs_count"]),
            "tx_count": int(row["tx_count"]),
            "duplicate_files_count": int(row["duplicate_files_count"]),
            "repos_count": len(repo_summaries),
        }
    finally:
        conn.close()


def normalize_path_prefix(path_prefix: str) -> str:
    normalized = path_prefix.strip().replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    if normalized == ".":
        return ""
    return normalized.rstrip("/")


def iter_path_prefixes(path_value: str) -> list[str]:
    normalized = normalize_path_prefix(path_value)
    if not normalized:
        return []
    parts = [part for part in normalized.split("/") if part]
    return ["/".join(parts[: index + 1]) for index in range(len(parts))]


def fetch_repo_summaries(root: Path, path_prefix: str) -> list[RepoSummary]:
    normalized_path = normalize_path_prefix(path_prefix)
    conn = connect_db(root)
    try:
        summary_rows = conn.execute(
            """
            SELECT
                COALESCE(fsg.git_repo_root, '') AS git_repo_root,
                COUNT(DISTINCT fsg.scan_id) AS scans_count,
                COUNT(DISTINCT fsg.file_id) AS files_count,
                COALESCE(MAX(sr.scan_time), '') AS latest_scan_time
            FROM file_scan_git fsg
            JOIN scan_run sr ON sr.scan_id = fsg.scan_id
            WHERE fsg.git_repo_root IS NOT NULL
            GROUP BY COALESCE(fsg.git_repo_root, '')
            ORDER BY COALESCE(fsg.git_repo_root, '') COLLATE NOCASE
            """
        ).fetchall()
        branch_rows = conn.execute(
            """
            SELECT DISTINCT
                COALESCE(git_repo_root, '') AS git_repo_root,
                COALESCE(git_branch, '(detached)') AS git_branch
            FROM file_scan_git
            WHERE git_repo_root IS NOT NULL
            ORDER BY COALESCE(git_repo_root, '') COLLATE NOCASE, COALESCE(git_branch, '(detached)') COLLATE NOCASE
            """
        ).fetchall()
    finally:
        conn.close()

    branches_by_repo: dict[str, list[str]] = {}
    for row in branch_rows:
        repo_root = str(row["git_repo_root"])
        branches_by_repo.setdefault(repo_root, []).append(str(row["git_branch"]))

    summaries: list[RepoSummary] = []
    for row in summary_rows:
        repo_root = str(row["git_repo_root"])
        if not prefix_matches_scope(normalized_path, repo_root):
            continue
        summaries.append(
            RepoSummary(
                repo_root=repo_root,
                branches=tuple(branches_by_repo.get(repo_root, [])),
                scans_count=int(row["scans_count"]),
                files_count=int(row["files_count"]),
                latest_scan_time=str(row["latest_scan_time"]),
            )
        )
    return summaries


def fetch_non_repo_root_summaries(root: Path, path_prefix: str) -> list[ScanRootSummary]:
    normalized_path = normalize_path_prefix(path_prefix)
    conn = connect_db(root)
    try:
        rows = conn.execute(
            """
            WITH normalized_scan_run AS (
                SELECT
                    COALESCE(NULLIF(scan_root, '.'), '') AS normalized_scan_root,
                    scan_id,
                    scan_time,
                    is_git_repo
                FROM scan_run
            ),
            latest_root_state AS (
                SELECT normalized_scan_root, is_git_repo
                FROM (
                    SELECT
                        normalized_scan_root,
                        is_git_repo,
                        ROW_NUMBER() OVER (
                            PARTITION BY normalized_scan_root
                            ORDER BY scan_time DESC, scan_id DESC
                        ) AS rn
                    FROM normalized_scan_run
                )
                WHERE rn = 1
            )
            SELECT
                nsr.normalized_scan_root AS scan_root,
                COUNT(*) AS scans_count,
                COALESCE(MAX(nsr.scan_time), '') AS latest_scan_time,
                (
                    SELECT COUNT(*)
                    FROM file_entry fe
                    WHERE fe.is_deleted = 0
                      AND (
                            nsr.normalized_scan_root = ''
                            OR COALESCE(fe.current_path, fe.canonical_uri) = nsr.normalized_scan_root
                            OR COALESCE(fe.current_path, fe.canonical_uri) LIKE nsr.normalized_scan_root || '/%'
                      )
                ) AS files_count
            FROM normalized_scan_run nsr
            JOIN latest_root_state lrs ON lrs.normalized_scan_root = nsr.normalized_scan_root
            WHERE lrs.is_git_repo = 0
            GROUP BY nsr.normalized_scan_root
            ORDER BY nsr.normalized_scan_root COLLATE NOCASE
            """
        ).fetchall()
    finally:
        conn.close()

    summaries: list[ScanRootSummary] = []
    for row in rows:
        scan_root = str(row["scan_root"])
        if not prefix_matches_scope(normalized_path, scan_root):
            continue
        summaries.append(
            ScanRootSummary(
                scan_root=scan_root,
                scans_count=int(row["scans_count"]),
                files_count=int(row["files_count"]),
                latest_scan_time=str(row["latest_scan_time"]),
            )
        )
    return summaries


def fetch_path_suggestions(root: Path, path_prefix: str, limit: int = 20) -> list[str]:
    normalized_path = normalize_path_prefix(path_prefix)
    normalized_lower = normalized_path.lower()
    path_like = f"{normalized_lower}%" if normalized_lower else "%"
    conn = connect_db(root)
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT COALESCE(current_path, canonical_uri) AS display_path
            FROM file_entry
            WHERE is_deleted = 0
              AND COALESCE(current_path, canonical_uri) IS NOT NULL
              AND lower(COALESCE(current_path, canonical_uri)) LIKE ?
            ORDER BY LENGTH(COALESCE(current_path, canonical_uri)), COALESCE(current_path, canonical_uri)
            LIMIT 500
            """,
            (path_like,),
        ).fetchall()
    finally:
        conn.close()

    directory_suggestions: set[str] = set()
    file_suggestions: set[str] = set()
    for row in rows:
        prefixes = iter_path_prefixes(str(row["display_path"]))
        if not prefixes:
            continue
        for prefix in prefixes[:-1]:
            if not normalized_lower or prefix.lower().startswith(normalized_lower):
                directory_suggestions.add(prefix)
        leaf = prefixes[-1]
        if not normalized_lower or leaf.lower().startswith(normalized_lower):
            file_suggestions.add(leaf)

    def sort_key(value: str) -> tuple[int, int, str]:
        return (value.count("/"), len(value), value.lower())

    ordered = sorted(directory_suggestions, key=sort_key) + sorted(file_suggestions, key=sort_key)
    return ordered[:limit]


def fetch_files(root: Path, query: str, path_prefix: str, branch: str, git_state: str) -> list[sqlite3.Row]:
    like = f"%{query.lower()}%"
    normalized_path = normalize_path_prefix(path_prefix).lower()
    path_like = f"{normalized_path}/%" if normalized_path else ""
    repo_ctx, active_branch = resolve_active_branch(root, path_prefix, branch)
    active_git_state = resolve_active_git_state(root, path_prefix, branch, git_state)
    cte = ""
    cte_params: tuple[object, ...] = ()
    join = ""
    git_state_select = "NULL AS git_state"
    git_state_filter = ""
    if repo_ctx is not None and active_branch:
        cte, cte_params = latest_branch_scope_cte(repo_ctx.repo_root, active_branch)
        join = "JOIN branch_scope bs ON bs.file_id = fe.file_id"
        git_state_select = "bs.git_state AS git_state"
        if active_git_state:
            git_state_filter = "AND bs.git_state = ?"
    conn = connect_db(root)
    try:
        return conn.execute(
            f"""
            {cte}
            SELECT
                fe.file_id,
                fe.canonical_uri,
                fe.current_path,
                fe.current_mime,
                fe.current_kind,
                fe.current_size_bytes,
                fe.current_hash,
                {git_state_select}
            FROM file_entry fe
            {join}
            WHERE is_deleted = 0
              {git_state_filter}
              AND (
                    ? = ''
                    OR lower(COALESCE(current_path, canonical_uri)) LIKE ?
                    OR lower(COALESCE(current_mime, '')) LIKE ?
                    OR lower(COALESCE(current_kind, '')) LIKE ?
                    OR lower(COALESCE(current_hash, '')) LIKE ?
                    OR EXISTS (
                        SELECT 1
                        FROM v_current_fact vcf
                        WHERE vcf.entity_id = fe.file_id
                          AND (
                                lower(COALESCE(vcf.value_text, '')) LIKE ?
                                OR lower(COALESCE(CAST(vcf.value_int AS TEXT), '')) LIKE ?
                                OR lower(COALESCE(vcf.value_json, '')) LIKE ?
                                OR lower(COALESCE(vcf.value_blobref, '')) LIKE ?
                              )
                    )
              )
              AND (
                    ? = ''
                    OR lower(COALESCE(current_path, canonical_uri)) = ?
                    OR lower(COALESCE(current_path, canonical_uri)) LIKE ?
                  )
            ORDER BY
                COALESCE(fe.current_mtime, '') DESC,
                lower(COALESCE(fe.current_path, fe.canonical_uri)) COLLATE NOCASE,
                fe.file_id DESC
            """,
            cte_params
            + ((active_git_state,) if active_git_state else ())
            + (query, like, like, like, like, like, like, like, like, normalized_path, normalized_path, path_like),
        ).fetchall()
    finally:
        conn.close()


def fetch_file_detail(root: Path, file_id: int, path_prefix: str, branch: str) -> tuple[Optional[sqlite3.Row], list[sqlite3.Row], list[sqlite3.Row]]:
    repo_ctx, active_branch = resolve_active_branch(root, path_prefix, branch)
    cte = ""
    cte_params: tuple[object, ...] = ()
    join = ""
    git_select = "NULL AS git_state, NULL AS git_status_raw"
    if repo_ctx is not None and active_branch:
        cte, cte_params = latest_branch_scope_cte(repo_ctx.repo_root, active_branch)
        join = "LEFT JOIN branch_scope bs ON bs.file_id = fe.file_id"
        git_select = "bs.git_state AS git_state, bs.git_status_raw AS git_status_raw"
    conn = connect_db(root)
    try:
        file_row = conn.execute(
            f"""
            {cte}
            SELECT
                fe.file_id,
                fe.canonical_uri,
                fe.current_path,
                fe.current_name,
                fe.current_extension,
                fe.current_mime,
                fe.current_kind,
                fe.current_hash,
                fe.current_size_bytes,
                fe.current_mtime,
                bo.storage_relpath,
                {git_select}
            FROM file_entry fe
            LEFT JOIN blob_object bo ON bo.blob_hash = fe.current_hash
            {join}
            WHERE fe.file_id = ?
            """,
            cte_params + (file_id,),
        ).fetchone()
        version_rows = conn.execute(
            """
            SELECT
                t.tx_id,
                t.tx_time,
                MAX(CASE WHEN a.ident = 'fs/path' THEN f.value_text END) AS path,
                MAX(CASE WHEN a.ident = 'fs/mime' THEN f.value_text END) AS mime,
                MAX(CASE WHEN a.ident = 'fs/kind' THEN f.value_text END) AS kind,
                MAX(CASE WHEN a.ident = 'fs/mtime' THEN f.value_text END) AS mtime,
                MAX(CASE WHEN a.ident = 'fs/size_bytes' THEN f.value_int END) AS size_bytes,
                MAX(CASE WHEN a.ident = 'fs/blob_hash' THEN f.value_blobref END) AS blob_hash
            FROM fact f
            JOIN tx t ON t.tx_id = f.tx_id
            JOIN attribute a ON a.attr_id = f.attr_id
            WHERE f.entity_id = ?
              AND f.added = 1
            GROUP BY t.tx_id, t.tx_time
            HAVING blob_hash IS NOT NULL
            ORDER BY t.tx_time DESC, t.tx_id DESC
            LIMIT 50
            """,
            (file_id,),
        ).fetchall()
        history_rows = conn.execute(
            """
            SELECT
                t.tx_id,
                t.tx_time,
                a.ident,
                f.added,
                f.value_text,
                f.value_int,
                f.value_json,
                f.value_blobref
            FROM fact f
            JOIN tx t ON t.tx_id = f.tx_id
            JOIN attribute a ON a.attr_id = f.attr_id
            WHERE f.entity_id = ?
            ORDER BY t.tx_time DESC, f.fact_id DESC
            LIMIT 200
            """,
            (file_id,),
        ).fetchall()
        return file_row, version_rows, history_rows
    finally:
        conn.close()


def fetch_matching_hash_rows(root: Path, file_id: int) -> tuple[Optional[sqlite3.Row], list[sqlite3.Row]]:
    conn = connect_db(root)
    try:
        file_row = conn.execute(
            """
            SELECT file_id, canonical_uri, current_hash, current_size_bytes
            FROM file_entry
            WHERE file_id = ?
            """,
            (file_id,),
        ).fetchone()
        if file_row is None or not file_row["current_hash"] or int(file_row["current_size_bytes"] or 0) == 0:
            return file_row, []
        rows = conn.execute(
            """
            SELECT
                fe.file_id,
                fe.canonical_uri,
                t.tx_time,
                MAX(CASE WHEN a.ident = 'fs/path' THEN f2.value_text END) AS path_at_time
            FROM fact f
            JOIN attribute a_hash ON a_hash.attr_id = f.attr_id AND a_hash.ident = 'fs/blob_hash'
            JOIN tx t ON t.tx_id = f.tx_id
            JOIN file_entry fe ON fe.file_id = f.entity_id
            JOIN fact f2 ON f2.entity_id = f.entity_id AND f2.tx_id = f.tx_id
            JOIN attribute a ON a.attr_id = f2.attr_id AND a.ident = 'fs/path'
            JOIN fact f_size ON f_size.entity_id = f.entity_id AND f_size.tx_id = f.tx_id
            JOIN attribute a_size ON a_size.attr_id = f_size.attr_id AND a_size.ident = 'fs/size_bytes'
            WHERE f.value_blobref = ?
              AND f.added = 1
              AND f_size.value_int > 0
            GROUP BY fe.file_id, fe.canonical_uri, t.tx_id, t.tx_time
            ORDER BY t.tx_time DESC, t.tx_id DESC
            LIMIT 200
            """,
            (file_row["current_hash"],),
        ).fetchall()
        return file_row, rows
    finally:
        conn.close()


def fetch_blobs(root: Path, query: str, path_prefix: str, branch: str, git_state: str) -> list[sqlite3.Row]:
    like = f"%{query.lower()}%"
    normalized_path = normalize_path_prefix(path_prefix).lower()
    path_like = f"{normalized_path}/%" if normalized_path else ""
    repo_ctx, active_branch = resolve_active_branch(root, path_prefix, branch)
    active_git_state = resolve_active_git_state(root, path_prefix, branch, git_state)
    cte = ""
    cte_params: tuple[object, ...] = ()
    exists_join = ""
    exists_state_filter = ""
    if repo_ctx is not None and active_branch:
        cte, cte_params = latest_branch_scope_cte(repo_ctx.repo_root, active_branch)
        exists_join = "JOIN branch_scope bs ON bs.file_id = fe.file_id"
        if active_git_state:
            exists_state_filter = "AND bs.git_state = ?"
    conn = connect_db(root)
    try:
        return conn.execute(
            f"""
            {cte}
            SELECT blob_hash, algo, size_bytes, storage_relpath, created_tx_id
            FROM blob_object
            WHERE (
                    ? = ''
                    OR lower(blob_hash) LIKE ?
                    OR lower(storage_relpath) LIKE ?
                  )
              AND (
                    ? = ''
                    OR EXISTS (
                        SELECT 1
                        FROM file_entry fe
                        {exists_join}
                        WHERE fe.is_deleted = 0
                          {exists_state_filter}
                          AND fe.current_hash = blob_object.blob_hash
                          AND (
                                lower(COALESCE(fe.current_path, fe.canonical_uri)) = ?
                                OR lower(COALESCE(fe.current_path, fe.canonical_uri)) LIKE ?
                          )
                    )
              )
            ORDER BY created_tx_id DESC, blob_hash DESC
            LIMIT 300
            """,
            cte_params
            + ((active_git_state,) if active_git_state else ())
            + (query, like, like, normalized_path, normalized_path, path_like),
        ).fetchall()
    finally:
        conn.close()


def fetch_duplicate_files(root: Path, query: str, path_prefix: str, branch: str, git_state: str) -> list[sqlite3.Row]:
    like = f"%{query.lower()}%"
    normalized_path = normalize_path_prefix(path_prefix).lower()
    path_like = f"{normalized_path}/%" if normalized_path else ""
    repo_ctx, active_branch = resolve_active_branch(root, path_prefix, branch)
    active_git_state = resolve_active_git_state(root, path_prefix, branch, git_state)
    cte = ""
    cte_params: tuple[object, ...] = ()
    scope_join = ""
    duplicate_join = ""
    file_state_filter = ""
    duplicate_state_filter = ""
    if repo_ctx is not None and active_branch:
        cte, cte_params = latest_branch_scope_cte(repo_ctx.repo_root, active_branch)
        scope_join = "JOIN branch_scope bs ON bs.file_id = fe.file_id"
        duplicate_join = "JOIN branch_scope bs_all ON bs_all.file_id = fe_all.file_id"
        if active_git_state:
            file_state_filter = "AND bs.git_state = ?"
            duplicate_state_filter = "AND bs_all.git_state = ?"
    conn = connect_db(root)
    try:
        return conn.execute(
            f"""
            {cte}
            WITH duplicate_hashes AS (
                SELECT fe_all.current_hash, COUNT(*) AS matches_count
                FROM file_entry fe_all
                {duplicate_join}
                WHERE fe_all.is_deleted = 0
                  AND fe_all.current_hash IS NOT NULL
                  AND COALESCE(fe_all.current_size_bytes, 0) > 0
                  {duplicate_state_filter}
                GROUP BY fe_all.current_hash
                HAVING COUNT(*) > 1
            ),
            filtered_scope AS (
                SELECT
                    fe.file_id,
                    fe.canonical_uri,
                    fe.current_path,
                    fe.current_hash,
                    fe.current_size_bytes
                FROM file_entry fe
                {scope_join}
                JOIN duplicate_hashes dh ON dh.current_hash = fe.current_hash
                WHERE fe.is_deleted = 0
                  AND fe.current_hash IS NOT NULL
                  AND COALESCE(fe.current_size_bytes, 0) > 0
                  {file_state_filter}
                  AND (
                        ? = ''
                        OR lower(COALESCE(fe.current_path, fe.canonical_uri)) = ?
                        OR lower(COALESCE(fe.current_path, fe.canonical_uri)) LIKE ?
                      )
                  AND (
                        ? = ''
                        OR lower(COALESCE(fe.current_path, fe.canonical_uri)) LIKE ?
                        OR lower(fe.current_hash) LIKE ?
                      )
            ),
            representative_files AS (
                SELECT current_hash, MIN(file_id) AS representative_file_id
                FROM filtered_scope
                GROUP BY current_hash
            )
            SELECT
                fs.file_id,
                fs.canonical_uri,
                fs.current_path,
                fs.current_hash,
                fs.current_size_bytes,
                dh.matches_count
            FROM filtered_scope fs
            JOIN representative_files rf ON rf.representative_file_id = fs.file_id
            JOIN duplicate_hashes dh ON dh.current_hash = fs.current_hash
            ORDER BY dh.matches_count DESC, lower(COALESCE(fs.current_path, fs.canonical_uri)), fs.file_id
            LIMIT 500
            """,
            cte_params
            + (
                *((active_git_state,) if active_git_state else ()),
                *((active_git_state,) if active_git_state else ()),
                *((active_git_state,) if active_git_state else ()),
                normalized_path,
                normalized_path,
                path_like,
                query,
                like,
                like,
            ),
        ).fetchall()
    finally:
        conn.close()


def fetch_transactions(root: Path, query: str, path_prefix: str, branch: str, git_state: str) -> list[sqlite3.Row]:
    like = f"%{query.lower()}%"
    normalized_path = normalize_path_prefix(path_prefix).lower()
    path_like = f"{normalized_path}/%" if normalized_path else ""
    repo_ctx, active_branch = resolve_active_branch(root, path_prefix, branch)
    active_git_state = resolve_active_git_state(root, path_prefix, branch, git_state)
    cte = ""
    cte_params: tuple[object, ...] = ()
    exists_join = ""
    exists_state_filter = ""
    if repo_ctx is not None and active_branch:
        cte, cte_params = latest_branch_scope_cte(repo_ctx.repo_root, active_branch)
        exists_join = "JOIN branch_scope bs ON bs.file_id = fe.file_id"
        if active_git_state:
            exists_state_filter = "AND bs.git_state = ?"
    conn = connect_db(root)
    try:
        return conn.execute(
            f"""
            {cte}
            SELECT tx_id, tx_time, actor, source, message
            FROM tx
            WHERE (
                    ? = ''
                    OR lower(COALESCE(actor, '')) LIKE ?
                    OR lower(COALESCE(source, '')) LIKE ?
                    OR lower(COALESCE(message, '')) LIKE ?
                    OR CAST(tx_id AS TEXT) = ?
                  )
              AND (
                    ? = ''
                    OR EXISTS (
                        SELECT 1
                        FROM fact f
                        JOIN file_entry fe ON fe.file_id = f.entity_id
                        {exists_join}
                        WHERE f.tx_id = tx.tx_id
                          AND fe.is_deleted = 0
                          {exists_state_filter}
                          AND (
                                lower(COALESCE(fe.current_path, fe.canonical_uri)) = ?
                                OR lower(COALESCE(fe.current_path, fe.canonical_uri)) LIKE ?
                          )
                    )
              )
            ORDER BY tx_id DESC
            LIMIT 300
            """,
            cte_params
            + ((active_git_state,) if active_git_state else ())
            + (query, like, like, like, query, normalized_path, normalized_path, path_like),
        ).fetchall()
    finally:
        conn.close()


def sql_query_authorizer(
    action_code: int,
    param1: Optional[str],
    param2: Optional[str],
    db_name: Optional[str],
    trigger_name: Optional[str],
) -> int:
    del param1, param2, db_name, trigger_name
    allowed_actions = {
        sqlite3.SQLITE_FUNCTION,
        sqlite3.SQLITE_READ,
        sqlite3.SQLITE_SELECT,
    }
    recursive_action = getattr(sqlite3, "SQLITE_RECURSIVE", None)
    if recursive_action is not None:
        allowed_actions.add(recursive_action)
    if action_code in allowed_actions:
        return sqlite3.SQLITE_OK
    return sqlite3.SQLITE_DENY


def execute_select_query(root: Path, sql_query: str) -> tuple[list[str], list[sqlite3.Row], bool, Optional[str]]:
    normalized_query = normalize_sql_query(sql_query)
    if not normalized_query:
        return [], [], False, None
    if not is_select_sql(normalized_query):
        return [], [], False, "Only SELECT queries are allowed."

    conn = connect_db_readonly(root)
    conn.set_authorizer(sql_query_authorizer)
    try:
        cursor = conn.execute(normalized_query)
        if cursor.description is None:
            return [], [], False, "Query did not return a result set."
        rows = cursor.fetchmany(SQL_QUERY_ROW_LIMIT + 1)
        truncated = len(rows) > SQL_QUERY_ROW_LIMIT
        if truncated:
            rows = rows[:SQL_QUERY_ROW_LIMIT]
        columns = [str(item[0]) for item in cursor.description]
        return columns, rows, truncated, None
    except sqlite3.Error as exc:
        message = str(exc)
        if "not authorized" in message.lower():
            message = "Only SELECT queries are allowed."
        return [], [], False, message
    finally:
        conn.close()


def fetch_file_ids_by_current_path(root: Path, current_paths: list[str]) -> dict[str, int]:
    normalized_paths = sorted({path for path in current_paths if path})
    if not normalized_paths:
        return {}
    placeholders = ", ".join("?" for _ in normalized_paths)
    conn = connect_db(root)
    try:
        rows = conn.execute(
            f"""
            SELECT file_id, current_path
            FROM file_entry
            WHERE is_deleted = 0
              AND current_path IN ({placeholders})
            ORDER BY file_id
            """,
            tuple(normalized_paths),
        ).fetchall()
    finally:
        conn.close()
    return {str(row["current_path"]): int(row["file_id"]) for row in rows if row["current_path"] is not None}


def lookup_blob_mime(root: Path, blob_hash: str) -> Optional[str]:
    conn = connect_db(root)
    try:
        row = conn.execute(
            """
            SELECT current_mime
            FROM file_entry
            WHERE current_hash = ?
              AND current_mime IS NOT NULL
            ORDER BY file_id
            LIMIT 1
            """,
            (blob_hash,),
        ).fetchone()
        if row is None:
            return None
        return str(row["current_mime"])
    finally:
        conn.close()


def render_layout(root: Path, initial_view: str, initial_path: str, initial_branch: str, initial_git_state: str, initial_content: str) -> str:
    shell = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>SCUM Browser</title>
  <script src="https://unpkg.com/htmx.org@1.9.12"></script>
  <style>
    :root {{
      color-scheme: light;
      --bg: #e7ecf4;
      --panel: #f8fafd;
      --surface: #ffffff;
      --ink: #273142;
      --muted: #677388;
      --line: #cfd7e4;
      --accent: #3164e8;
      --accent-soft: #e9efff;
      --chip: #eef2f8;
      --shadow: 0 18px 42px rgba(45, 61, 89, 0.10);
      --mono: "Iosevka", "SFMono-Regular", ui-monospace, monospace;
      --sans: "IBM Plex Sans", "Segoe UI", sans-serif;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: var(--sans);
      color: var(--ink);
      background:
        linear-gradient(180deg, #cfd9ea 0, #cfd9ea 12px, transparent 12px),
        radial-gradient(circle at top center, rgba(255, 255, 255, 0.8), transparent 34rem),
        linear-gradient(180deg, #f2f5fa 0%, var(--bg) 100%);
    }}
    a {{ color: var(--accent); }}
    .page {{
      max-width: 1400px;
      margin: 0 auto;
      padding: 24px;
    }}
    .hero {{
      display: grid;
      gap: 18px;
      grid-template-columns: 1.3fr 1fr;
      align-items: start;
      margin-bottom: 18px;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 16px;
      box-shadow: var(--shadow);
    }}
    .hero-card {{
      padding: 24px;
    }}
    .title {{
      margin: 0;
      font-size: clamp(2rem, 5vw, 3.2rem);
      line-height: 0.95;
      letter-spacing: -0.04em;
    }}
    .subtitle {{
      color: var(--muted);
      max-width: 45rem;
    }}
    .stats {{
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(4, 1fr);
    }}
    .stat {{
      padding: 16px;
      border: 1px solid #d9e0ec;
      border-radius: 14px;
      background: var(--chip);
    }}
    .stat-link {{
      text-align: left;
      font: inherit;
      color: inherit;
      cursor: pointer;
    }}
    .stat strong {{
      display: block;
      font-size: 1.4rem;
    }}
    .nav {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 18px;
    }}
    .nav button {{
      border: 1px solid var(--line);
      background: var(--surface);
      color: var(--ink);
      padding: 10px 14px;
      border-radius: 999px;
      font: inherit;
      cursor: pointer;
      font-weight: 600;
      box-shadow: 0 1px 0 rgba(255, 255, 255, 0.75) inset;
    }}
    .nav button.active {{
      background: var(--accent);
      color: white;
      border-color: var(--accent);
    }}
    .content {{
      min-height: 60vh;
      padding: 20px;
    }}
    .split {{
      display: grid;
      grid-template-columns: minmax(280px, 420px) minmax(0, 1fr);
      gap: 18px;
      align-items: start;
    }}
    .stack {{
      display: grid;
      gap: 14px;
      align-content: start;
    }}
    .toolbar {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: flex-start;
    }}
    .toolbar input,
    .toolbar textarea {{
      flex: 1 1 240px;
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px 12px;
      font: inherit;
      background: var(--surface);
      color: var(--ink);
      box-shadow: 0 1px 0 rgba(255, 255, 255, 0.85) inset;
    }}
    .toolbar select {{
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px 12px;
      font: inherit;
      background: var(--surface);
      color: var(--ink);
      min-width: 11rem;
      box-shadow: 0 1px 0 rgba(255, 255, 255, 0.85) inset;
    }}
    .toolbar textarea {{
      min-height: 10rem;
      resize: vertical;
      font-family: var(--mono);
    }}
    .toolbar input:focus,
    .toolbar textarea:focus,
    .toolbar select:focus {{
      outline: 2px solid rgba(49, 100, 232, 0.18);
      outline-offset: 1px;
      border-color: rgba(49, 100, 232, 0.55);
    }}
    .toolbar button {{
      border: 1px solid var(--accent);
      background: var(--accent);
      color: white;
      border-radius: 12px;
      padding: 10px 14px;
      font: inherit;
      cursor: pointer;
      font-weight: 600;
      box-shadow: 0 1px 0 rgba(255, 255, 255, 0.18) inset;
    }}
    .list {{
      max-height: 68vh;
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: var(--surface);
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.85);
    }}
    .list-item {{
      width: 100%;
      text-align: left;
      background: transparent;
      border: 0;
      border-bottom: 1px solid #e2e7f0;
      padding: 14px 16px;
      cursor: pointer;
      font: inherit;
    }}
    .list-item:hover {{
      background: #f3f6fc;
    }}
    .list-item.active {{
      background: #e8efff;
    }}
    .list-item strong {{
      display: block;
      font-family: var(--mono);
      font-size: 0.92rem;
      word-break: break-all;
    }}
    .repo-link {{
      padding: 0;
      border: 0;
      background: transparent;
      color: var(--accent);
      cursor: pointer;
      font: inherit;
      text-align: left;
    }}
    .repo-link strong {{
      display: inline;
      font-family: var(--mono);
      font-size: 0.92rem;
      word-break: break-all;
    }}
    .meta, table {{
      width: 100%;
      border-collapse: collapse;
    }}
    .meta td, .meta th, table td, table th {{
      border-bottom: 1px solid #e2e7f0;
      text-align: left;
      padding: 10px 8px;
      vertical-align: top;
    }}
    .meta td:first-child, table th {{
      width: 12rem;
      color: var(--muted);
      font-weight: 600;
    }}
    .detail-disclosure {{
      margin-top: 12px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.45);
      overflow: hidden;
    }}
    .detail-disclosure summary {{
      display: flex;
      align-items: center;
      gap: 0.35rem;
      cursor: pointer;
      padding: 12px 14px;
      font-weight: 600;
      color: var(--accent);
      user-select: none;
    }}
    .detail-disclosure summary:hover {{
      background: rgba(49, 100, 232, 0.05);
    }}
    .detail-disclosure summary::-webkit-details-marker {{
      display: none;
    }}
    .detail-disclosure .label-open {{
      display: none;
    }}
    .detail-disclosure[open] .label-closed {{
      display: none;
    }}
    .detail-disclosure[open] .label-open {{
      display: inline;
    }}
    .disclosure-body {{
      padding: 0 14px 14px;
    }}
    .detail-disclosure .meta {{
      margin-top: 0;
    }}
    .mono {{
      font-family: var(--mono);
      word-break: break-all;
    }}
    .empty {{
      padding: 28px;
      color: var(--muted);
      text-align: center;
      border: 1px dashed var(--line);
      border-radius: 14px;
      background: #f6f8fc;
    }}
    .card {{
      border: 1px solid var(--line);
      border-radius: 16px;
      background: var(--surface);
      padding: 18px;
    }}
    .code-block {{
      margin: 0;
      padding: 16px;
      overflow: auto;
      background: #202833;
      color: #edf3fb;
      border-radius: 14px;
      font-family: var(--mono);
      font-size: 0.9rem;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      word-break: break-word;
    }}
    .tag {{
      display: inline-block;
      padding: 4px 8px;
      border-radius: 999px;
      background: #edf2f8;
      color: #5f6d84;
      font-size: 0.85rem;
      margin-right: 8px;
      border: 1px solid #dbe2ec;
    }}
    .diff-added {{
      color: #b8f5c8;
    }}
    .diff-removed {{
      color: #ffb6b6;
    }}
    .diff-meta {{
      color: #9fb1c9;
    }}
    .error {{
      color: #a12c2c;
      background: #fff0f0;
      border-color: #efc6c6;
    }}
    .notice {{
      border: 1px solid #cfe2cf;
      background: #f2fbf2;
      color: #255c2f;
    }}
    .notice strong {{
      display: block;
      margin-bottom: 4px;
    }}
    .notice.error {{
      color: #a12c2c;
      background: #fff0f0;
      border-color: #efc6c6;
    }}
    .root-actions {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }}
    .root-watch {{
      display: inline-flex;
      align-items: center;
    }}
    .watch-toggle {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      color: var(--ink);
      font-weight: 600;
      white-space: nowrap;
    }}
    .watch-toggle input {{
      width: 1rem;
      height: 1rem;
      margin: 0;
      accent-color: var(--accent);
    }}
    .watch-status {{
      color: var(--muted);
      font-size: 0.9rem;
      white-space: nowrap;
    }}
    .root-actions button {{
      border: 1px solid var(--line);
      background: var(--surface);
      color: var(--ink);
      border-radius: 10px;
      padding: 8px 10px;
      font: inherit;
      cursor: pointer;
      font-weight: 600;
    }}
    .root-actions .danger {{
      border-color: #d99a9a;
      color: #8a2424;
      background: #fff6f6;
    }}
    @media (max-width: 980px) {{
      .hero, .split, .stats {{
        grid-template-columns: 1fr;
      }}
      .content {{
        padding: 16px;
      }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <div class="panel hero-card">
        <p class="tag">SQLite metadata + preserved blobs</p>
        <h1 class="title">SCUM Browser</h1>
        <p class="subtitle">Browse the current database state, inspect immutable fact history, and jump straight to preserved blob bytes.</p>
        {render_path_filter(root, initial_path, initial_branch, initial_git_state)}
        <div class="nav">
          {nav_button("roots", initial_view)}
          {nav_button("repos", initial_view)}
          {nav_button("files", initial_view)}
          {nav_button("duplicates", initial_view)}
          {nav_button("blobs", initial_view)}
          {nav_button("tx", initial_view)}
          {nav_button("sql", initial_view)}
        </div>
      </div>
      {render_stats_panel(root, initial_path, initial_branch, initial_git_state)}
    </section>
    <section id="content" class="panel content">{initial_content}</section>
  </div>
  <script>
    (() => {{
      const storagePrefix = "scum:";

      function bindPersistentDisclosures(root) {{
        root.querySelectorAll("details[data-pref-key]").forEach((element) => {{
          const storageKey = storagePrefix + element.dataset.prefKey;
          try {{
            const stored = window.localStorage.getItem(storageKey);
            if (stored === "open") {{
              element.open = true;
            }} else if (stored === "closed") {{
              element.open = false;
            }}
          }} catch (_error) {{
          }}

          if (element.dataset.prefBound === "1") {{
            return;
          }}
          element.addEventListener("toggle", () => {{
            try {{
              window.localStorage.setItem(storageKey, element.open ? "open" : "closed");
            }} catch (_error) {{
            }}
          }});
          element.dataset.prefBound = "1";
        }});
      }}

      document.addEventListener("DOMContentLoaded", () => bindPersistentDisclosures(document));
      document.addEventListener("htmx:afterSwap", (event) => bindPersistentDisclosures(event.target));
    }})();
  </script>
</body>
</html>
"""
    return shell


def nav_button(name: str, active: str) -> str:
    labels = {
        "roots": "Roots",
        "repos": "Repos",
        "files": "Files",
        "duplicates": "Duplicates",
        "blobs": "Blobs",
        "tx": "Transactions",
        "sql": "SQL",
    }
    active_class = "active" if name == active else ""
    return (
        f'<button class="{active_class}" hx-get="/partials/{name}" '
        f'hx-include="#path-filter" hx-target="#content" hx-swap="innerHTML">{labels[name]}</button>'
    )


def render_path_filter(root: Path, path_prefix: str, branch: str, git_state: str, oob: bool = False) -> str:
    repo_ctx, active_branch = resolve_active_branch(root, path_prefix, branch)
    active_git_state = resolve_active_git_state(root, path_prefix, branch, git_state)
    branch_html = ""
    state_html = ""
    if repo_ctx is not None:
        display_repo_root = repo_ctx.repo_root or "."
        options = ['<option value="">All branches</option>']
        for branch_name in repo_ctx.branches:
            selected = ' selected="selected"' if branch_name == active_branch else ""
            options.append(f'<option value="{h(branch_name)}"{selected}>{h(branch_name)}</option>')
        branch_html = (
            f'<span class="subtitle">Repo: <span class="mono">{h(display_repo_root)}</span></span>'
            f'<select id="branch-input" name="branch" aria-label="Scanned branch">'
            + "".join(options)
            + "</select>"
        )
        if active_branch:
            state_options = ['<option value="">All states</option>']
            for option in GIT_STATE_OPTIONS:
                selected = ' selected="selected"' if option == active_git_state else ""
                state_options.append(f'<option value="{h(option)}"{selected}>{h(option.title())}</option>')
            state_html = (
                f'<select id="git-state-input" name="git_state" aria-label="Git state">'
                + "".join(state_options)
                + "</select>"
            )
    oob_attr = ' hx-swap-oob="outerHTML"' if oob else ""
    return f"""
<form id="path-filter" class="toolbar" hx-get="/partials/files" hx-target="#content" hx-swap="innerHTML"{oob_attr}>
  <input
    id="path-input"
    type="search"
    name="path"
    value="{h(path_prefix)}"
    list="path-suggestions"
    autocomplete="off"
    spellcheck="false"
    placeholder="Limit to a repo path prefix like examples/demo"
    hx-get="/partials/path-suggestions"
    hx-trigger="focus, input changed delay:120ms"
    hx-target="#path-suggestions"
    hx-swap="innerHTML"
  >
  <datalist id="path-suggestions">{render_path_suggestions(root, path_prefix)}</datalist>
  {branch_html}
  {state_html}
  <button type="submit">Apply Path</button>
</form>
"""


def render_stats_panel(root: Path, path_prefix: str, branch: str, git_state: str, oob: bool = False) -> str:
    repo_ctx, active_branch = resolve_active_branch(root, path_prefix, branch)
    active_git_state = resolve_active_git_state(root, path_prefix, branch, git_state)
    stats = fetch_stats(root, path_prefix, active_branch, active_git_state)
    normalized_path = normalize_path_prefix(path_prefix)
    scope_label = f"Scoped to {normalized_path}" if normalized_path else "Whole repository"
    if repo_ctx is not None and active_branch:
        scope_label = f"{scope_label} @ {active_branch}"
    if active_git_state:
        scope_label = f"{scope_label} [{active_git_state}]"
    oob_attr = ' hx-swap-oob="outerHTML"' if oob else ""
    return f"""
<div id="stats-panel" class="panel hero-card"{oob_attr}>
  <div class="stats">
    <div class="stat"><span>Files</span><strong>{stats['files_count']}</strong></div>
    <button class="stat stat-link" hx-get="/partials/duplicates" hx-include="#path-filter" hx-target="#content" hx-swap="innerHTML">
      <span>Duplicate Files</span><strong>{stats['duplicate_files_count']}</strong>
    </button>
    <div class="stat"><span>Blobs</span><strong>{stats['blobs_count']}</strong></div>
    <div class="stat"><span>Transactions</span><strong>{stats['tx_count']}</strong></div>
    <button class="stat stat-link" hx-get="/partials/repos" hx-include="#path-filter" hx-target="#content" hx-swap="innerHTML">
      <span>Repos Scanned</span><strong>{stats['repos_count']}</strong>
    </button>
  </div>
  <p class="subtitle">Stats scope: <span class="mono">{h(scope_label)}</span></p>
  <p class="subtitle">Repository root: <span class="mono">{h(root)}</span></p>
</div>
"""


def render_partial_response(root: Path, path_prefix: str, branch: str, git_state: str, content: str) -> str:
    return render_path_filter(root, path_prefix, branch, git_state, oob=True) + render_stats_panel(root, path_prefix, branch, git_state, oob=True) + content


def render_path_suggestions(root: Path, path_prefix: str) -> str:
    return "".join(f'<option value="{h(suggestion)}"></option>' for suggestion in fetch_path_suggestions(root, path_prefix))


def build_browser_url(
    view: str,
    path_prefix: str,
    branch: str,
    git_state: str,
    query: str = "",
    selected_file_id: Optional[int] = None,
    sql_query: str = "",
) -> str:
    params: list[tuple[str, str]] = [("view", normalize_view_name(view))]
    normalized_path = normalize_path_prefix(path_prefix)
    normalized_branch = normalize_branch_name(branch)
    normalized_git_state = git_state.strip().lower()
    normalized_query = query.strip()
    normalized_sql_query = normalize_sql_query(sql_query)
    if normalized_path:
        params.append(("path", normalized_path))
    if normalized_branch:
        params.append(("branch", normalized_branch))
    if normalized_git_state:
        params.append(("git_state", normalized_git_state))
    if normalized_query:
        params.append(("q", normalized_query))
    if normalize_view_name(view) == "sql" and normalized_sql_query:
        params.append(("sql", normalized_sql_query))
    if normalize_view_name(view) == "files" and selected_file_id is not None:
        params.append(("file", str(selected_file_id)))
    return "/?" + urlencode(params)


def render_files_partial(
    root: Path,
    query: str,
    path_prefix: str,
    branch: str,
    git_state: str,
    selected_file_id: Optional[int] = None,
) -> str:
    rows = fetch_files(root, query, path_prefix, branch, git_state)
    active_file_id: Optional[int] = None
    if rows:
        row_ids = {int(row["file_id"]) for row in rows}
        if selected_file_id is not None and selected_file_id in row_ids:
            active_file_id = selected_file_id
        else:
            active_file_id = int(rows[0]["file_id"])
        detail_html = render_file_detail(root, active_file_id, path_prefix, branch)
    else:
        detail_html = empty_state("No files matched this query.")
    items = []
    for row in rows:
        file_id = int(row["file_id"])
        display_path = row["current_path"] or row["canonical_uri"]
        git_state = row["git_state"]
        git_meta = f' · <span class="tag">{h(git_state)}</span>' if git_state else ""
        active_class = " active" if file_id == active_file_id else ""
        items.append(
            f"""
<button class="list-item{active_class}" hx-get="/partials/files/{file_id}" hx-include="#path-filter, #files-query-form" hx-target="#file-detail" hx-swap="innerHTML">
  <strong>{h(display_path)}</strong>
  <div>{h(row["current_mime"] or "-")} · {h(row["current_kind"] or "-")} · {fmt_bytes(row["current_size_bytes"])}{git_meta}</div>
  <div class="mono">{h((row["current_hash"] or "")[:16])}</div>
</button>
"""
        )
    listing = "".join(items) if items else empty_state("No tracked files yet.")
    return f"""
<div class="split">
  <section class="stack">
    <form id="files-query-form" class="toolbar" hx-get="/partials/files" hx-include="#path-filter" hx-target="#content" hx-swap="innerHTML">
      <input type="search" name="q" value="{h(query)}" placeholder="Filter by path, type, hash, or metadata">
      <button type="submit">Search</button>
    </form>
    <div class="list">{listing}</div>
  </section>
  <section id="file-detail">{detail_html}</section>
</div>
"""


def render_file_detail(root: Path, file_id: int, path_prefix: str, branch: str) -> str:
    _, active_branch = resolve_active_branch(root, path_prefix, branch)
    file_row, version_rows, history_rows = fetch_file_detail(root, file_id, path_prefix, branch)
    if file_row is None:
        return empty_state(f"File entity {file_id} was not found.")

    blob_hash = file_row["current_hash"]
    blob_link = (
        f'<a href="/blob/{quote(blob_hash)}" target="_blank" rel="noreferrer">open raw blob</a>'
        if blob_hash
        else "-"
    )
    preview = render_blob_preview(root, blob_hash, file_row["current_kind"], file_row["current_mime"]) if blob_hash else ""
    versions_html = render_version_history_table(version_rows, blob_hash)
    history_html = render_history_table(history_rows)
    if blob_hash:
        matching_hash_action = (
            f'<button type="button" hx-get="/partials/files/{file_id}/matching-hashes" '
            f'hx-target="#matching-hash-results" hx-swap="innerHTML">Find matching hashes</button>'
        )
        matching_hash_results = '<p class="subtitle">Find duplicates and earlier observed locations for this blob hash.</p>'
    else:
        matching_hash_action = ""
        matching_hash_results = empty_state("No preserved blob hash is available for this file.")
    branch_rows = ""
    if active_branch:
        branch_rows = (
            f'<tr><td>Branch</td><td>{h(active_branch)}</td></tr>'
            f'<tr><td>Git state</td><td>{h(file_row["git_state"] or "-")}</td></tr>'
            f'<tr><td>Git raw status</td><td class="mono">{h(file_row["git_status_raw"] or "-")}</td></tr>'
        )
    return f"""
<div class="stack">
  <div class="card">
    <h2>{h(file_row["current_path"] or file_row["canonical_uri"])}</h2>
    <table class="meta">
      <tr><td>Modified</td><td>{h(file_row["current_mtime"] or "-")}</td></tr>
    </table>
    <details class="detail-disclosure">
      <summary><span class="label-closed">Show Details</span><span class="label-open">Hide Details</span></summary>
      <div class="disclosure-body">
        <table class="meta">
        <tr><td>File ID</td><td class="mono">{h(file_row["file_id"])}</td></tr>
        <tr><td>Canonical URI</td><td class="mono">{h(file_row["canonical_uri"])}</td></tr>
        <tr><td>Name</td><td>{h(file_row["current_name"])}</td></tr>
        <tr><td>Extension</td><td>{h(file_row["current_extension"] or "-")}</td></tr>
        <tr><td>MIME</td><td>{h(file_row["current_mime"] or "-")}</td></tr>
        <tr><td>Kind</td><td>{h(file_row["current_kind"] or "-")}</td></tr>
        <tr><td>Size</td><td>{fmt_bytes(file_row["current_size_bytes"])}</td></tr>
        <tr><td>Blob hash</td><td class="mono">{h(blob_hash or "-")}</td></tr>
        <tr><td>Blob path</td><td class="mono">{h(file_row["storage_relpath"] or "-")}</td></tr>
        <tr><td>Blob</td><td>{blob_link}</td></tr>
        {branch_rows}
      </table>
      </div>
    </details>
  </div>
  <div id="blob-preview-panel">{preview}</div>
  <div class="card">
    <h3>Matching Hashes</h3>
    <div class="toolbar">{matching_hash_action}</div>
    <div id="matching-hash-results">{matching_hash_results}</div>
  </div>
  <div class="card">
    <h3>Version History</h3>
    {versions_html}
  </div>
  <div class="card">
    <h3>Fact History</h3>
    {history_html}
  </div>
</div>
"""


def is_probably_text(path: Path) -> bool:
    try:
        sample = path.read_bytes()[:4096]
    except OSError:
        return False
    if not sample:
        return True
    if b"\x00" in sample:
        return False
    try:
        sample.decode("utf-8")
        return True
    except UnicodeDecodeError:
        pass
    printable = sum(
        1
        for byte in sample
        if byte in (9, 10, 13) or 32 <= byte <= 126
    )
    return printable / len(sample) > 0.9


def read_text_blob(path: Path, limit: int = 65536) -> Optional[str]:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            return handle.read(limit)
    except OSError:
        return None


def render_blob_preview(root: Path, blob_hash: str, kind: Optional[str], mime: Optional[str]) -> str:
    path = blob_abspath(root, blob_hash)
    if not path.exists():
        return ""
    blob_url = f"/blob/{quote(blob_hash)}"

    if mime and mime.startswith("image/"):
        preview_body = f'<img src="{blob_url}" alt="Blob preview" style="max-width: 100%; height: auto; border-radius: 12px;">'
    elif mime and mime.startswith("audio/"):
        preview_body = f'<audio controls preload="metadata" src="{blob_url}" style="width: 100%;"></audio>'
    elif mime and mime.startswith("video/"):
        preview_body = f'<video controls preload="metadata" src="{blob_url}" style="width: 100%; max-height: 32rem; border-radius: 12px;"></video>'
    elif mime == "application/pdf":
        preview_body = (
            f'<iframe src="{blob_url}" title="Blob preview" '
            f'style="width: 100%; min-height: 32rem; border: 0; border-radius: 12px;"></iframe>'
        )
    elif kind == "text" or is_probably_text(path):
        text = read_text_blob(path, limit=4096)
        if text is None:
            return ""
        suffix = "\n…" if path.stat().st_size > len(text.encode("utf-8", errors="ignore")) else ""
        preview_body = f'<pre class="code-block">{h(text)}{suffix}</pre>'
    else:
        preview_body = (
            f'<p class="subtitle">No inline preview for this blob type. '
            f'Use <a href="{blob_url}" target="_blank" rel="noreferrer">open raw blob</a>.</p>'
        )

    return f"""
<div class="card">
  <h3>Blob Preview</h3>
  <details class="detail-disclosure" data-pref-key="blob-preview-visible" open>
    <summary><span class="label-closed">Show Blob Preview</span><span class="label-open">Hide Blob Preview</span></summary>
    <div class="disclosure-body">
      {preview_body}
    </div>
  </details>
</div>
"""


def render_blob_diff(
    root: Path,
    newer_blob_hash: str,
    older_blob_hash: str,
    newer_kind: Optional[str],
    newer_mime: Optional[str],
    older_kind: Optional[str],
    older_mime: Optional[str],
    newer_label: str,
    older_label: str,
) -> str:
    newer_path = blob_abspath(root, newer_blob_hash)
    older_path = blob_abspath(root, older_blob_hash)
    if not newer_path.exists() or not older_path.exists():
        return empty_state("One of the preserved blobs for this diff is missing.")

    newer_is_text = newer_kind == "text" or is_probably_text(newer_path)
    older_is_text = older_kind == "text" or is_probably_text(older_path)
    if not newer_is_text or not older_is_text:
        return empty_state("Inline diff is only available for text blobs.")

    newer_text = read_text_blob(newer_path)
    older_text = read_text_blob(older_path)
    if newer_text is None or older_text is None:
        return empty_state("Could not read one of the preserved text blobs.")

    diff_lines = list(
        difflib.unified_diff(
            older_text.splitlines(),
            newer_text.splitlines(),
            fromfile=older_label,
            tofile=newer_label,
            lineterm="",
        )
    )
    if not diff_lines:
        diff_lines = ["No content changes between these preserved versions."]

    rendered_lines = []
    for line in diff_lines:
        line_class = ""
        if line.startswith(("---", "+++", "@@")):
            line_class = ' class="diff-meta"'
        elif line.startswith("+") and not line.startswith("+++"):
            line_class = ' class="diff-added"'
        elif line.startswith("-") and not line.startswith("---"):
            line_class = ' class="diff-removed"'
        rendered_lines.append(f"<span{line_class}>{h(line)}</span>")

    newer_blob_url = f"/blob/{quote(newer_blob_hash)}"
    older_blob_url = f"/blob/{quote(older_blob_hash)}"
    summary = f'{h(older_label)} -> {h(newer_label)}'
    if newer_mime or older_mime:
        summary = f'{summary} · {h(newer_mime or older_mime or "text/plain")}'
    return f"""
<div class="card">
  <h3>Blob Diff</h3>
  <p class="subtitle">{summary} · <a href="{older_blob_url}" target="_blank" rel="noreferrer">older raw</a> · <a href="{newer_blob_url}" target="_blank" rel="noreferrer">newer raw</a></p>
  <pre class="code-block">{"\n".join(rendered_lines)}</pre>
</div>
"""


def render_version_history_table(rows: list[sqlite3.Row], current_blob_hash: Optional[str]) -> str:
    if not rows:
        return empty_state("No preserved versions recorded for this file.")
    body = []
    for index, row in enumerate(rows):
        blob_hash = row["blob_hash"]
        is_current = blob_hash == current_blob_hash
        current_label = " current" if is_current else ""
        previous_row = rows[index + 1] if index + 1 < len(rows) else None
        preview_link = (
            f'<a href="#" hx-get="/partials/blob-preview/{quote(blob_hash)}?kind={quote(row["kind"] or "")}&mime={quote(row["mime"] or "")}" '
            f'hx-target="#blob-preview-panel" hx-swap="innerHTML">preview</a>'
            if blob_hash
            else "-"
        )
        diff_link = "-"
        if blob_hash and previous_row is not None and previous_row["blob_hash"]:
            diff_link = (
                f'<a href="#" hx-get="/partials/blob-diff?newer={quote(blob_hash)}&older={quote(previous_row["blob_hash"])}'
                f'&newer_kind={quote(row["kind"] or "")}&newer_mime={quote(row["mime"] or "")}'
                f'&older_kind={quote(previous_row["kind"] or "")}&older_mime={quote(previous_row["mime"] or "")}'
                f'&newer_label={quote(row["tx_time"] or blob_hash[:12])}&older_label={quote(previous_row["tx_time"] or previous_row["blob_hash"][:12])}" '
                f'hx-target="#blob-preview-panel" hx-swap="innerHTML">diff prev</a>'
            )
        raw_link = (
            f'<a href="/blob/{quote(blob_hash)}" target="_blank" rel="noreferrer">raw</a>'
            if blob_hash
            else "-"
        )
        hash_cell = (
            f'<span class="mono">{h(blob_hash[:16])}</span><span class="subtitle">{current_label}</span>'
            if blob_hash
            else "-"
        )
        body.append(
            f"""
<tr>
  <th>{h(row["tx_time"])}</th>
  <td>{fmt_bytes(row["size_bytes"])}</td>
  <td>{h(row["mime"] or row["kind"] or "-")}</td>
  <td class="mono">{hash_cell}</td>
  <td>{preview_link} · {diff_link} · {raw_link}</td>
</tr>
"""
        )
    return "<table><thead><tr><th>Time</th><th>Size</th><th>Type</th><th>Blob</th><th>Actions</th></tr></thead><tbody>" + "".join(body) + "</tbody></table>"


def render_matching_hashes_partial(root: Path, file_id: int) -> str:
    file_row, rows = fetch_matching_hash_rows(root, file_id)
    if file_row is None:
        return empty_state(f"File entity {file_id} was not found.")
    if int(file_row["current_size_bytes"] or 0) == 0:
        return empty_state("Empty files are excluded from hash-match reporting.")
    if not file_row["current_hash"]:
        return empty_state("No preserved blob hash is available for this file.")
    if not rows:
        return empty_state("No matching blob hashes were found.")

    body = []
    for row in rows:
        file_link = (
            f'<a class="mono" href="#" hx-get="/partials/files/{row["file_id"]}" '
            f'hx-include="#path-filter, #files-query-form" hx-target="#file-detail" hx-swap="innerHTML">'
            f"{h(row['canonical_uri'])}</a>"
        )
        current_label = '<span class="subtitle"> current file</span>' if row["file_id"] == file_id else ""
        body.append(
            f"""
<tr>
  <th>{file_link}{current_label}</th>
  <td>{h(row["path_at_time"] or "-")}</td>
  <td>{h(row["tx_time"])}</td>
</tr>
"""
        )
    return (
        '<p class="subtitle">Showing every transaction where this blob hash was observed.</p>'
        + "<table><thead><tr><th>File</th><th>Observed Path</th><th>Time</th></tr></thead><tbody>"
        + "".join(body)
        + "</tbody></table>"
    )


def render_history_table(rows: list[sqlite3.Row]) -> str:
    if not rows:
        return empty_state("No facts recorded for this entity.")
    body = []
    for row in rows:
        value = row["value_text"]
        if value is None:
            value = row["value_int"]
        if value is None:
            value = row["value_json"]
        if value is None:
            value = row["value_blobref"]
        sign = "+" if row["added"] else "-"
        rendered_value = h(value)
        if row["ident"] == "fs/blob_hash" and value:
            rendered_value = (
                f'<a class="mono" href="/blob/{quote(str(value))}" target="_blank" rel="noreferrer">{h(value)}</a>'
            )
        elif row["value_json"] is not None:
            try:
                pretty_json = json.dumps(json.loads(row["value_json"]), indent=2, sort_keys=True)
            except json.JSONDecodeError:
                pretty_json = str(row["value_json"])
            rendered_value = f"<pre class=\"code-block\">{h(pretty_json)}</pre>"
        body.append(
            f"<tr><th>{h(row['tx_time'])}</th><td class='mono'>{h(row['ident'])}</td><td>{sign}</td><td class='mono'>{rendered_value}</td></tr>"
        )
    return "<table><thead><tr><th>Time</th><th>Attribute</th><th>Op</th><th>Value</th></tr></thead><tbody>" + "".join(body) + "</tbody></table>"


def render_blobs_partial(root: Path, query: str, path_prefix: str, branch: str, git_state: str) -> str:
    rows = fetch_blobs(root, query, path_prefix, branch, git_state)
    if rows:
        body = "".join(
            f"""
<tr>
  <th class="mono">{h(row["blob_hash"])}</th>
  <td>{fmt_bytes(row["size_bytes"])}</td>
  <td class="mono">{h(row["storage_relpath"])}</td>
  <td class="mono">{h(row["created_tx_id"])}</td>
  <td><a href="/blob/{quote(row["blob_hash"])}" target="_blank" rel="noreferrer">open</a></td>
</tr>
"""
            for row in rows
        )
        table = "<table><thead><tr><th>Blob Hash</th><th>Size</th><th>Path</th><th>TX</th><th>Raw</th></tr></thead><tbody>" + body + "</tbody></table>"
    else:
        table = empty_state("No blobs matched this query.")
    return f"""
<div class="stack">
  <form class="toolbar" hx-get="/partials/blobs" hx-include="#path-filter" hx-target="#content" hx-swap="innerHTML">
    <input type="search" name="q" value="{h(query)}" placeholder="Filter by blob hash or storage path">
    <button type="submit">Search</button>
  </form>
  <div class="card">{table}</div>
</div>
"""


def render_duplicates_partial(root: Path, query: str, path_prefix: str, branch: str, git_state: str) -> str:
    rows = fetch_duplicate_files(root, query, path_prefix, branch, git_state)
    if rows:
        body = "".join(
            f"""
<tr>
  <th><a class="mono" href="{h(build_browser_url('files', path_prefix, branch, git_state, selected_file_id=int(row['file_id'])))}">{h(row["current_path"] or row["canonical_uri"])}</a></th>
  <td>{h(row["matches_count"])}</td>
  <td>{fmt_bytes(row["current_size_bytes"])}</td>
  <td class="mono">{h(row["current_hash"])}</td>
</tr>
"""
            for row in rows
        )
        summary = (
            f'<p class="subtitle">Returned {len(rows)} duplicate hash group'
            f'{"s" if len(rows) != 1 else ""}. Each row shows one representative current file for a shared blob hash.</p>'
        )
        table = (
            "<table><thead><tr><th>File</th><th>Matches</th><th>Size</th><th>Blob Hash</th></tr></thead><tbody>"
            + body
            + "</tbody></table>"
        )
    else:
        summary = '<p class="subtitle">No current files with shared blob hashes matched this scope.</p>'
        table = empty_state("No duplicate files matched this query.")
    return f"""
<div class="stack">
  <form class="toolbar" hx-get="/partials/duplicates" hx-include="#path-filter" hx-target="#content" hx-swap="innerHTML">
    <input type="search" name="q" value="{h(query)}" placeholder="Filter duplicate files by path or hash">
    <button type="submit">Show Duplicates</button>
  </form>
  {summary}
  <div class="card">{table}</div>
</div>
"""


def render_repos_partial(root: Path, path_prefix: str) -> str:
    rows = fetch_repo_summaries(root, path_prefix)
    if rows:
        body = "".join(
            f"""
<tr>
  <th class="mono">
    <button
      class="repo-link"
      hx-get="/partials/files?path={quote(row.repo_root or '.')}"
      hx-target="#content"
      hx-swap="innerHTML"
    ><strong>{h(row.repo_root or '.')}</strong></button>
  </th>
  <td>{h(', '.join(row.branches) or '-')}</td>
  <td>{row.files_count}</td>
  <td>{row.scans_count}</td>
  <td>{h(row.latest_scan_time or '-')}</td>
</tr>
"""
            for row in rows
        )
        table = "<table><thead><tr><th>Repo Root</th><th>Branches</th><th>Files</th><th>Scans</th><th>Latest Scan</th></tr></thead><tbody>" + body + "</tbody></table>"
    else:
        table = empty_state("No scanned git repos matched this scope.")
    return f"""
<div class="stack">
  <div class="card">{table}</div>
</div>
"""


def render_roots_partial(root: Path, path_prefix: str, message: Optional[ActionMessage] = None) -> str:
    rows = fetch_non_repo_root_summaries(root, path_prefix)
    notice = render_action_notice(message)
    watch_manager = ROOT_WATCH_MANAGER
    watch_summaries = watch_manager.snapshot() if watch_manager is not None else {}
    watch_detail = (
        watch_manager.availability_detail()
        if watch_manager is not None
        else "Changed files are scanned after a 60 second stability window."
    )
    def watch_summary(scan_root: str) -> RootWatchSummary:
        return watch_summaries.get(scan_root, RootWatchSummary(scan_root, False, 0))
    if rows:
        body = "".join(
            f"""
<tr>
  <th class="mono">
    <button
      class="repo-link"
      hx-get="/partials/files?path={quote(row.scan_root or '.')}"
      hx-target="#content"
      hx-swap="innerHTML"
    ><strong>{h(row.scan_root or '.')}</strong></button>
  </th>
  <td>{row.files_count}</td>
  <td>{row.scans_count}</td>
  <td>{h(row.latest_scan_time or '-')}</td>
  <td>
    <form class="root-watch" hx-post="/actions/root-watch" hx-trigger="change" hx-include="#path-filter" hx-target="#content" hx-swap="innerHTML">
      <input type="hidden" name="root" value="{h(row.scan_root or '.')}">
      <label class="watch-toggle">
        <input type="checkbox" name="enabled" value="1"{" checked" if watch_summary(row.scan_root or ".").active else ""}>
        <span>Watch</span>
      </label>
    </form>
    <div class="watch-status">{h(f"{watch_summary(row.scan_root or '.').pending_files} pending" if watch_summary(row.scan_root or '.').active else "off")}</div>
  </td>
  <td>
    <form class="root-actions" hx-post="/actions/root" hx-include="#path-filter" hx-target="#content" hx-swap="innerHTML">
      <input type="hidden" name="root" value="{h(row.scan_root or '.')}">
      <button type="submit" name="action" value="scan">Scan</button>
      <button type="submit" class="danger" name="action" value="forget" hx-confirm="Forget all tracked data for {h(row.scan_root or '.')}?">Forget</button>
    </form>
  </td>
</tr>
"""
            for row in rows
        )
        table = "<table><thead><tr><th>Scan Root</th><th>Files</th><th>Scans</th><th>Latest Scan</th><th>Watch</th><th>Actions</th></tr></thead><tbody>" + body + "</tbody></table>"
    else:
        table = empty_state("No scanned non-repo roots matched this scope.")
    return f"""
<div class="stack">
  {notice}
  <p class="subtitle">{h(watch_detail)}</p>
  <div class="card">{table}</div>
</div>
"""


def render_tx_partial(root: Path, query: str, path_prefix: str, branch: str, git_state: str) -> str:
    rows = fetch_transactions(root, query, path_prefix, branch, git_state)
    if rows:
        body = "".join(
            f"""
<tr>
  <th>{h(row["tx_id"])}</th>
  <td>{h(row["tx_time"])}</td>
  <td>{h(row["actor"] or "-")}</td>
  <td>{h(row["source"] or "-")}</td>
  <td>{h(row["message"] or "-")}</td>
</tr>
"""
            for row in rows
        )
        table = "<table><thead><tr><th>TX</th><th>Time</th><th>Actor</th><th>Source</th><th>Message</th></tr></thead><tbody>" + body + "</tbody></table>"
    else:
        table = empty_state("No transactions matched this query.")
    return f"""
<div class="stack">
  <form class="toolbar" hx-get="/partials/tx" hx-include="#path-filter" hx-target="#content" hx-swap="innerHTML">
    <input type="search" name="q" value="{h(query)}" placeholder="Filter by actor, source, message, or tx id">
    <button type="submit">Search</button>
  </form>
  <div class="card">{table}</div>
</div>
"""


def render_sql_value(value: object) -> str:
    if value is None:
        return '<span class="subtitle">NULL</span>'
    if isinstance(value, bytes):
        return h(value.hex())
    return h(value)


def render_sql_cell(
    column: str,
    value: object,
    current_path_to_file_id: dict[str, int],
    path_prefix: str,
    branch: str,
    git_state: str,
) -> str:
    if column == "current_path" and isinstance(value, str):
        file_id = current_path_to_file_id.get(value)
        if file_id is not None:
            return (
                f'<a class="mono" href="{h(build_browser_url("files", path_prefix, branch, git_state, selected_file_id=file_id))}">'
                f"{h(value)}</a>"
            )
    return render_sql_value(value)


def render_sql_partial(root: Path, sql_query: str, path_prefix: str, branch: str, git_state: str) -> str:
    normalized_query = normalize_sql_query(sql_query)
    columns, rows, truncated, error = execute_select_query(root, normalized_query)
    if error is not None:
        results_html = f'<div class="card error"><strong>Query rejected</strong><p>{h(error)}</p></div>'
    elif not normalized_query:
        results_html = (
            '<div class="card">'
            '<p class="subtitle">Run a read-only SQL query against <span class="mono">.sysmvp.db</span>. '
            'Only <span class="mono">SELECT</span> statements are allowed.</p>'
            f'<pre class="code-block">{h(SQL_QUERY_DEFAULT)}</pre>'
            "</div>"
        )
    elif not rows:
        results_html = (
            '<div class="stack">'
            '<div class="card"><p class="subtitle">Query returned 0 rows.</p></div>'
            "</div>"
        )
    else:
        current_path_to_file_id = fetch_file_ids_by_current_path(
            root,
            [
                str(row["current_path"])
                for row in rows
                if "current_path" in row.keys() and row["current_path"] is not None
            ],
        )
        header_html = "".join(f"<th>{h(column)}</th>" for column in columns)
        body_rows = []
        for row in rows:
            cells = "".join(
                f"<td>{render_sql_cell(column, row[column], current_path_to_file_id, path_prefix, branch, git_state)}</td>"
                for column in columns
            )
            body_rows.append(f"<tr>{cells}</tr>")
        truncation_note = (
            f'<p class="subtitle">Showing the first {SQL_QUERY_ROW_LIMIT} rows. Add <span class="mono">LIMIT</span> to narrow the result.</p>'
            if truncated
            else f'<p class="subtitle">Returned {len(rows)} row{"s" if len(rows) != 1 else ""}.</p>'
        )
        results_html = truncation_note + "<div class=\"card\"><table><thead><tr>" + header_html + "</tr></thead><tbody>" + "".join(body_rows) + "</tbody></table></div>"
    return f"""
<div class="stack">
  <form class="toolbar" hx-get="/partials/sql" hx-include="#path-filter" hx-target="#content" hx-swap="innerHTML">
    <textarea name="sql" spellcheck="false" placeholder="SELECT current_path FROM file_entry ORDER BY current_path LIMIT 20">{h(normalized_query)}</textarea>
    <button type="submit">Run Query</button>
  </form>
  <p class="subtitle">SQL runs exactly as written against the current database. Path and branch filters are preserved in the URL, but they do not rewrite your SQL.</p>
  {results_html}
</div>
"""


def empty_state(message: str) -> str:
    return f'<div class="empty">{h(message)}</div>'


def render_action_notice(message: Optional[ActionMessage]) -> str:
    if message is None:
        return ""
    level_class = " error" if message.level == "error" else ""
    detail_html = f"<p>{h(message.detail)}</p>" if message.detail else ""
    return (
        f'<div class="card notice{level_class}">'
        f"<strong>{h(message.title)}</strong>"
        f"{detail_html}"
        "</div>"
    )


def parse_query(handler: BaseHTTPRequestHandler) -> tuple[str, dict[str, list[str]]]:
    parsed = urlparse(handler.path)
    return parsed.path, parse_qs(parsed.query)


def parse_form(handler: BaseHTTPRequestHandler) -> dict[str, list[str]]:
    try:
        content_length = int(handler.headers.get("Content-Length", "0"))
    except ValueError:
        content_length = 0
    payload = handler.rfile.read(content_length).decode("utf-8")
    return parse_qs(payload)


def query_value(query: dict[str, list[str]], key: str) -> str:
    return query.get(key, [""])[0].strip()


def query_int_value(query: dict[str, list[str]], key: str) -> Optional[int]:
    raw_value = query_value(query, key)
    if not raw_value.isdigit():
        return None
    return int(raw_value)


def render_root_content(
    root: Path,
    view: str,
    query: str,
    sql_query: str,
    path_prefix: str,
    branch: str,
    git_state: str,
    selected_file_id: Optional[int],
) -> str:
    if view == "repos":
        return render_repos_partial(root, path_prefix)
    if view == "roots":
        return render_roots_partial(root, path_prefix)
    if view == "duplicates":
        return render_duplicates_partial(root, query, path_prefix, branch, git_state)
    if view == "blobs":
        return render_blobs_partial(root, query, path_prefix, branch, git_state)
    if view == "tx":
        return render_tx_partial(root, query, path_prefix, branch, git_state)
    if view == "sql":
        return render_sql_partial(root, sql_query, path_prefix, branch, git_state)
    return render_files_partial(root, query, path_prefix, branch, git_state, selected_file_id)


class BrowserHandler(BaseHTTPRequestHandler):
    repo_root: Path

    def do_GET(self) -> None:  # noqa: N802
        path, query = parse_query(self)
        view = normalize_view_name(query_value(query, "view"))
        path_prefix = query_value(query, "path")
        branch = query_value(query, "branch")
        git_state = query_value(query, "git_state")
        text_query = query_value(query, "q")
        sql_query = query_value(query, "sql")
        selected_file_id = query_int_value(query, "file")
        if path == "/":
            content = render_root_content(
                self.repo_root,
                view,
                text_query,
                sql_query,
                path_prefix,
                branch,
                git_state,
                selected_file_id,
            )
            self.respond_html(render_layout(self.repo_root, view, path_prefix, branch, git_state, content))
            return
        if path == "/partials/files":
            content = render_files_partial(self.repo_root, text_query, path_prefix, branch, git_state, selected_file_id)
            self.respond_html(
                render_partial_response(self.repo_root, path_prefix, branch, git_state, content),
                headers={"HX-Push-Url": build_browser_url("files", path_prefix, branch, git_state, text_query, selected_file_id)},
            )
            return
        if path == "/partials/repos":
            content = render_repos_partial(self.repo_root, path_prefix)
            self.respond_html(
                render_partial_response(self.repo_root, path_prefix, branch, git_state, content),
                headers={"HX-Push-Url": build_browser_url("repos", path_prefix, branch, git_state)},
            )
            return
        if path == "/partials/roots":
            content = render_roots_partial(self.repo_root, path_prefix)
            self.respond_html(
                render_partial_response(self.repo_root, path_prefix, branch, git_state, content),
                headers={"HX-Push-Url": build_browser_url("roots", path_prefix, branch, git_state)},
            )
            return
        if path == "/partials/duplicates":
            content = render_duplicates_partial(self.repo_root, text_query, path_prefix, branch, git_state)
            self.respond_html(
                render_partial_response(self.repo_root, path_prefix, branch, git_state, content),
                headers={"HX-Push-Url": build_browser_url("duplicates", path_prefix, branch, git_state, text_query)},
            )
            return
        if path == "/partials/path-suggestions":
            self.respond_html(render_path_suggestions(self.repo_root, path_prefix))
            return
        if path.startswith("/partials/files/"):
            tail = path.removeprefix("/partials/files/")
            if tail.endswith("/matching-hashes"):
                file_id_text = tail.removesuffix("/matching-hashes").rstrip("/")
                if file_id_text.isdigit():
                    self.respond_html(render_matching_hashes_partial(self.repo_root, int(file_id_text)))
                    return
            if tail.isdigit():
                file_id = int(tail)
                self.respond_html(
                    render_file_detail(self.repo_root, file_id, path_prefix, branch),
                    headers={"HX-Push-Url": build_browser_url("files", path_prefix, branch, git_state, text_query, file_id)},
                )
                return
        if path.startswith("/partials/blob-preview/"):
            blob_hash = path.removeprefix("/partials/blob-preview/")
            kind = query_value(query, "kind") or None
            mime = query_value(query, "mime") or None
            self.respond_html(render_blob_preview(self.repo_root, blob_hash, kind, mime))
            return
        if path == "/partials/blob-diff":
            newer_blob_hash = query_value(query, "newer")
            older_blob_hash = query_value(query, "older")
            newer_kind = query_value(query, "newer_kind") or None
            newer_mime = query_value(query, "newer_mime") or None
            older_kind = query_value(query, "older_kind") or None
            older_mime = query_value(query, "older_mime") or None
            newer_label = query_value(query, "newer_label") or newer_blob_hash[:12]
            older_label = query_value(query, "older_label") or older_blob_hash[:12]
            self.respond_html(
                render_blob_diff(
                    self.repo_root,
                    newer_blob_hash,
                    older_blob_hash,
                    newer_kind,
                    newer_mime,
                    older_kind,
                    older_mime,
                    newer_label,
                    older_label,
                )
            )
            return
        if path == "/partials/blobs":
            content = render_blobs_partial(self.repo_root, text_query, path_prefix, branch, git_state)
            self.respond_html(
                render_partial_response(self.repo_root, path_prefix, branch, git_state, content),
                headers={"HX-Push-Url": build_browser_url("blobs", path_prefix, branch, git_state, text_query)},
            )
            return
        if path == "/partials/tx":
            content = render_tx_partial(self.repo_root, text_query, path_prefix, branch, git_state)
            self.respond_html(
                render_partial_response(self.repo_root, path_prefix, branch, git_state, content),
                headers={"HX-Push-Url": build_browser_url("tx", path_prefix, branch, git_state, text_query)},
            )
            return
        if path == "/partials/sql":
            content = render_sql_partial(self.repo_root, sql_query, path_prefix, branch, git_state)
            self.respond_html(
                render_partial_response(self.repo_root, path_prefix, branch, git_state, content),
                headers={"HX-Push-Url": build_browser_url("sql", path_prefix, branch, git_state, sql_query=sql_query)},
            )
            return
        if path.startswith("/blob/"):
            blob_hash = path.removeprefix("/blob/")
            self.respond_blob(blob_hash)
            return
        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def do_POST(self) -> None:  # noqa: N802
        path, _ = parse_query(self)
        form = parse_form(self)
        path_prefix = query_value(form, "path")
        branch = query_value(form, "branch")
        git_state = query_value(form, "git_state")
        if path == "/actions/root-watch":
            root_value = query_value(form, "root")
            if not root_value:
                self.respond_html(
                    render_partial_response(
                        self.repo_root,
                        path_prefix,
                        branch,
                        git_state,
                        render_roots_partial(
                            self.repo_root,
                            path_prefix,
                            ActionMessage("error", "Watch action failed", "Missing root."),
                        ),
                    ),
                    status=HTTPStatus.OK,
                    headers={"HX-Push-Url": build_browser_url("roots", path_prefix, branch, git_state)},
                )
                return
            enabled = query_value(form, "enabled") == "1"
            manager = ROOT_WATCH_MANAGER
            if manager is None:
                message = ActionMessage("error", "Watch action failed", "Watch manager is not available.")
            else:
                message = manager.set_enabled(root_value, enabled)
            content = render_roots_partial(self.repo_root, path_prefix, message)
            self.respond_html(
                render_partial_response(self.repo_root, path_prefix, branch, git_state, content),
                status=HTTPStatus.OK,
                headers={"HX-Push-Url": build_browser_url("roots", path_prefix, branch, git_state)},
            )
            return
        if path == "/actions/root":
            action = query_value(form, "action")
            root_value = query_value(form, "root")
            if not root_value:
                self.respond_html(
                    render_partial_response(
                        self.repo_root,
                        path_prefix,
                        branch,
                        git_state,
                        render_roots_partial(
                            self.repo_root,
                            path_prefix,
                            ActionMessage("error", "Root action failed", "Missing root."),
                        ),
                    ),
                    status=HTTPStatus.OK,
                    headers={"HX-Push-Url": build_browser_url("roots", path_prefix, branch, git_state)},
                )
                return
            message = run_sysmvp_action(self.repo_root, action, root_value)
            content = render_roots_partial(self.repo_root, path_prefix, message)
            self.respond_html(
                render_partial_response(self.repo_root, path_prefix, branch, git_state, content),
                status=HTTPStatus.OK,
                headers={"HX-Push-Url": build_browser_url("roots", path_prefix, branch, git_state)},
            )
            return
        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def respond_html(
        self,
        body: str,
        status: HTTPStatus = HTTPStatus.OK,
        headers: Optional[dict[str, str]] = None,
    ) -> None:
        encoded = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        if headers:
            for key, value in headers.items():
                self.send_header(key, value)
        self.end_headers()
        self.wfile.write(encoded)

    def respond_blob(self, blob_hash: str) -> None:
        if not blob_hash or "/" in blob_hash or len(blob_hash) < 3:
            self.send_error(HTTPStatus.BAD_REQUEST, "Invalid blob hash")
            return
        path = blob_abspath(self.repo_root, blob_hash)
        if not path.exists() or not path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND, "Blob not found")
            return
        stat_result = path.stat()
        content_type = lookup_blob_mime(self.repo_root, blob_hash)
        if content_type is None:
            content_type = mimetypes.guess_type(str(path.name))[0] or "application/octet-stream"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(stat_result.st_size))
        self.send_header("Content-Disposition", f'inline; filename="{blob_hash}"')
        self.end_headers()
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(1024 * 64)
                if not chunk:
                    break
                self.wfile.write(chunk)

    def log_message(self, fmt: str, *args: object) -> None:
        sys.stderr.write("[sysbrowse] " + fmt % args + "\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Browse the SCUM SQLite metadata and preserved blobs with HTMX")
    parser.add_argument("--repo", default=".", help="Repository root containing .sysmvp.db")
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind")
    parser.add_argument("--port", default=8000, type=int, help="Port to bind")
    return parser


def main() -> int:
    global ROOT_WATCH_MANAGER
    parser = build_parser()
    args = parser.parse_args()
    root = repo_root_from(Path(args.repo))
    ensure_repo_exists(root)
    ROOT_WATCH_MANAGER = RootWatchManager(root)

    handler_cls = type("ConfiguredBrowserHandler", (BrowserHandler,), {"repo_root": root})
    with ThreadingHTTPServer((args.host, args.port), handler_cls) as httpd:
        host, port = httpd.server_address
        log_browser(f"Serving SCUM browser at http://{host}:{port}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            return 0
        finally:
            if ROOT_WATCH_MANAGER is not None:
                ROOT_WATCH_MANAGER.stop_all()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
