#!/usr/bin/env python3
import argparse
import datetime as dt
import fnmatch
import hashlib
import json
import mimetypes
import os
import shutil
import sqlite3
import subprocess
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

DB_NAME = ".sysmvp.db"
STORE_DIR = ".sysstore/objects"
SCHEMA_FILE = "schema.sql"
IGNORE_FILE = ".sysignore"
EXTENSIONS_FILE = ".sysextensions.json"
EXTRACTORS_DIR = "extractors"
EXTENSION_MANIFEST = "extension.json"
XMP_NS = {
    "dc": "http://purl.org/dc/elements/1.1/",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
}


@dataclass(frozen=True)
class GitFileStatus:
    repo_rel_path: str
    git_status_raw: str
    git_state: str


@dataclass(frozen=True)
class GitScanContext:
    is_git_repo: bool
    repo_root_abs: Optional[Path]
    git_repo_root: Optional[str]
    git_branch: Optional[str]
    git_head: Optional[str]
    statuses: dict[str, GitFileStatus]


@dataclass(frozen=True)
class ExtensionConfig:
    name: str
    entrypoint: Path
    mime_prefixes: tuple[str, ...]
    file_patterns: tuple[str, ...]
    attr_ident: str
    attr_description: str


def log(message: str) -> None:
    print(f"[sysmvp] {message}", file=sys.stderr)


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


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


def ensure_repo_exists(root: Path) -> None:
    if not db_path(root).exists():
        raise SystemExit(f"Repository not initialized at {root}. Run: python3 sysmvp.py init")


def read_text_file(path: Path) -> str:
    with path.open("r", encoding="utf-8") as handle:
        return handle.read()


def normalize_rel_path(path_value: str) -> str:
    normalized = path_value.replace(os.sep, "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    if normalized == ".":
        return ""
    return normalized.rstrip("/")


def path_matches_patterns(path_value: str, patterns: Iterable[str]) -> bool:
    normalized = path_value.replace(os.sep, "/")
    basename = Path(normalized).name
    return any(fnmatch.fnmatch(normalized, pattern) or fnmatch.fnmatch(basename, pattern) for pattern in patterns)


def scope_matches_path(path_value: str, scope: str) -> bool:
    if scope == "":
        return True
    return path_value == scope or path_value.startswith(scope + "/")


def normalize_scope_arg(root: Path, scope_value: str) -> str:
    raw = scope_value.strip()
    if not raw:
        raise SystemExit("Scope must not be empty")
    candidate = Path(raw)
    if candidate.is_absolute():
        return normalize_rel_path(os.path.relpath(candidate.resolve(), root))
    return normalize_rel_path(raw)


def default_extensions_config() -> dict[str, object]:
    return {
        "extensions": {
            "asciidoc_header": {
                "enabled": False,
            },
            "image_metadata": {
                "enabled": False,
            },
            "picasa_ini": {
                "enabled": False,
            }
        }
    }


def write_extensions_config(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def ensure_extensions_config(root: Path) -> None:
    config_path = root / EXTENSIONS_FILE
    if config_path.exists():
        return
    source_config = Path(__file__).resolve().parent / EXTENSIONS_FILE
    if source_config.exists():
        shutil.copy2(source_config, config_path)
        log(f"Created default {EXTENSIONS_FILE}")
        return
    write_extensions_config(config_path, default_extensions_config())
    log(f"Created default {EXTENSIONS_FILE}")


def read_extensions_config(root: Path) -> dict[str, object]:
    ensure_extensions_config(root)
    config_path = root / EXTENSIONS_FILE
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise SystemExit(f"Failed to read {EXTENSIONS_FILE}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON in {EXTENSIONS_FILE}: {exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit(f"{EXTENSIONS_FILE} must contain a JSON object")
    return payload


def extension_settings_by_name(root: Path) -> dict[str, object]:
    payload = read_extensions_config(root)
    extensions = payload.get("extensions", {})
    if not isinstance(extensions, dict):
        raise SystemExit(f"{EXTENSIONS_FILE} field 'extensions' must be a JSON object")
    return extensions


def extension_dir_path(root: Path, extension_name: str) -> Path:
    bundled = Path(__file__).resolve().parent / EXTRACTORS_DIR / extension_name
    if bundled.exists():
        return bundled
    return root / EXTRACTORS_DIR / extension_name


def extension_manifest_path(root: Path, extension_name: str) -> Path:
    return extension_dir_path(root, extension_name) / EXTENSION_MANIFEST


def read_extension_manifest(root: Path, extension_name: str) -> dict[str, object]:
    manifest_path = extension_manifest_path(root, extension_name)
    if not manifest_path.exists():
        raise SystemExit(f"Extension manifest not found for '{extension_name}': {manifest_path}")
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise SystemExit(f"Failed to read extension manifest for '{extension_name}': {exc}") from exc
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON in extension manifest for '{extension_name}': {exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit(f"Extension manifest for '{extension_name}' must contain a JSON object")
    return payload


def parse_string_list(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise SystemExit(f"{label} must be a JSON array of strings")
    values: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise SystemExit(f"{label} must contain only strings")
        values.append(item)
    return tuple(values)


def parse_string_field(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise SystemExit(f"{label} must be a non-empty string")
    return value


def load_extension_config(root: Path, extension_name: str) -> Optional[ExtensionConfig]:
    settings_by_name = extension_settings_by_name(root)
    raw_settings = settings_by_name.get(extension_name)
    if raw_settings is None:
        return None
    if isinstance(raw_settings, bool):
        enabled = raw_settings
        settings: dict[str, object] = {}
    elif isinstance(raw_settings, dict):
        enabled = raw_settings.get("enabled", False)
        if not isinstance(enabled, bool):
            raise SystemExit(f"{EXTENSIONS_FILE} extension '{extension_name}' has non-boolean 'enabled'")
        settings = raw_settings
    else:
        raise SystemExit(f"{EXTENSIONS_FILE} extension '{extension_name}' must be a boolean or object")
    if not enabled:
        return None

    manifest = read_extension_manifest(root, extension_name)
    raw_entrypoint = manifest.get("entrypoint")
    if not isinstance(raw_entrypoint, str) or not raw_entrypoint:
        raise SystemExit(f"Extension manifest for '{extension_name}' must define string field 'entrypoint'")
    raw_attribute = manifest.get("attribute")
    if not isinstance(raw_attribute, dict):
        raise SystemExit(f"Extension manifest for '{extension_name}' must define object field 'attribute'")

    raw_mime_prefixes = settings.get("mime_prefixes", manifest.get("mime_prefixes", []))
    raw_file_patterns = settings.get("file_patterns", manifest.get("file_patterns", []))
    mime_prefixes = parse_string_list(raw_mime_prefixes, f"{extension_name}.mime_prefixes")
    file_patterns = parse_string_list(raw_file_patterns, f"{extension_name}.file_patterns")
    attr_ident = parse_string_field(raw_attribute.get("ident"), f"{extension_name}.attribute.ident")
    raw_attr_description = raw_attribute.get("description", f"Extracted JSON for extension '{extension_name}'")
    attr_description = parse_string_field(raw_attr_description, f"{extension_name}.attribute.description")

    return ExtensionConfig(
        name=extension_name,
        entrypoint=Path(raw_entrypoint),
        mime_prefixes=mime_prefixes,
        file_patterns=file_patterns,
        attr_ident=attr_ident,
        attr_description=attr_description,
    )


def load_enabled_extensions(root: Path) -> list[ExtensionConfig]:
    configs: list[ExtensionConfig] = []
    for extension_name in sorted(extension_settings_by_name(root)):
        config = load_extension_config(root, extension_name)
        if config is not None:
            configs.append(config)
    return configs


def mime_matches_prefixes(mime: str, prefixes: Iterable[str]) -> bool:
    return any(mime.startswith(prefix) for prefix in prefixes)


def init_repo(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    log(f"Initializing repository at {root}")
    (root / ".sysstore").mkdir(exist_ok=True)
    store_root(root).mkdir(parents=True, exist_ok=True)

    schema_path = Path(__file__).resolve().parent / SCHEMA_FILE
    if not schema_path.exists():
        schema_path = root / SCHEMA_FILE
    if not schema_path.exists():
        raise SystemExit(f"Schema file not found: {schema_path}")

    conn = connect_db(root)
    try:
        conn.executescript(read_text_file(schema_path))
        tx_id = create_tx(conn, actor="system", source="init", message="initialize repository")
        seed_attributes(conn, tx_id)
        conn.commit()
    finally:
        conn.close()

    ignore_path = root / IGNORE_FILE
    if not ignore_path.exists():
        source_ignore = Path(__file__).resolve().parent / IGNORE_FILE
        if source_ignore.exists():
            shutil.copy2(source_ignore, ignore_path)
            log(f"Created default {IGNORE_FILE}")
        else:
            ignore_path.write_text(".git/\n.sysstore/\n", encoding="utf-8")
            log(f"Created minimal {IGNORE_FILE}")

    ensure_extensions_config(root)
    log("Repository initialized")


def create_tx(conn: sqlite3.Connection, actor: str, source: str, message: str) -> int:
    cur = conn.execute(
        "INSERT INTO tx (tx_time, actor, source, message) VALUES (?, ?, ?, ?)",
        (utc_now(), actor, source, message),
    )
    return int(cur.lastrowid)


def seed_attributes(conn: sqlite3.Connection, tx_id: int) -> None:
    del tx_id  # kept for future hooks and symmetry
    attrs = [
        ("fs/path", "text", "one", "Observed filesystem path"),
        ("fs/name", "text", "one", "Basename"),
        ("fs/extension", "text", "one", "Filename extension"),
        ("fs/mime", "text", "one", "Detected MIME type"),
        ("fs/kind", "text", "one", "Broad kind such as text or binary"),
        ("fs/hash", "text", "one", "Content hash"),
        ("fs/size_bytes", "int", "one", "File size in bytes"),
        ("fs/mtime", "text", "one", "Filesystem modified time"),
        ("fs/blob_hash", "blobref", "one", "Preserved blob hash"),
        ("image/metadata", "json", "one", "Embedded image metadata"),
    ]
    conn.executemany(
        """
        INSERT OR IGNORE INTO attribute (ident, value_type, cardinality, description)
        VALUES (?, ?, ?, ?)
        """,
        attrs,
    )


def ensure_extension_attributes(conn: sqlite3.Connection, extension_configs: Iterable[ExtensionConfig]) -> None:
    attrs = [
        (extension.attr_ident, "json", "one", extension.attr_description)
        for extension in extension_configs
    ]
    if not attrs:
        return
    conn.executemany(
        """
        INSERT OR IGNORE INTO attribute (ident, value_type, cardinality, description)
        VALUES (?, ?, ?, ?)
        """,
        attrs,
    )


def get_attr_id(conn: sqlite3.Connection, ident: str) -> int:
    row = conn.execute("SELECT attr_id FROM attribute WHERE ident = ?", (ident,)).fetchone()
    if row is None:
        raise SystemExit(f"Unknown attribute: {ident}")
    return int(row["attr_id"])


def get_attr_info(conn: sqlite3.Connection, ident: str) -> tuple[int, str]:
    row = conn.execute("SELECT attr_id, value_type FROM attribute WHERE ident = ?", (ident,)).fetchone()
    if row is None:
        raise SystemExit(f"Unknown attribute: {ident}")
    return int(row["attr_id"]), str(row["value_type"])


def load_ignore_patterns(root: Path) -> list[str]:
    ignore_path = root / IGNORE_FILE
    patterns: list[str] = []
    if not ignore_path.exists():
        return patterns
    for line in ignore_path.read_text(encoding="utf-8").splitlines():
        value = line.strip()
        if not value or value.startswith("#"):
            continue
        patterns.append(value)
    log(f"Loaded {len(patterns)} ignore pattern(s)")
    return patterns


def is_ignored(rel_path: str, patterns: Iterable[str]) -> bool:
    normalized = rel_path.replace(os.sep, "/")
    parts = normalized.split("/")
    for pattern in patterns:
        p = pattern.strip()
        if p.endswith("/"):
            dirname = p[:-1]
            if dirname and dirname in parts:
                return True
            if normalized.startswith(p):
                return True
        if fnmatch.fnmatch(normalized, p):
            return True
        if fnmatch.fnmatch(Path(normalized).name, p):
            return True
    return False


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def detect_mime(path: Path) -> str:
    mime, _ = mimetypes.guess_type(str(path))
    return mime or "application/octet-stream"


def classify_kind(mime: str) -> str:
    if mime.startswith("text/"):
        return "text"
    if mime.startswith("image/"):
        return "image"
    if mime.startswith("audio/"):
        return "audio"
    if mime.startswith("video/"):
        return "video"
    if mime in ("application/json", "application/xml", "application/yaml"):
        return "text"
    if mime in ("application/zip", "application/x-tar", "application/gzip"):
        return "archive"
    if mime == "application/pdf":
        return "document"
    return "binary"


def normalize_extracted_text(value: str) -> Optional[str]:
    normalized = " ".join(value.split())
    if not normalized:
        return None
    return normalized


def extract_xmp_description(payload: bytes) -> Optional[str]:
    start = 0
    closing_tag = b"</x:xmpmeta>"
    while True:
        open_index = payload.find(b"<x:xmpmeta", start)
        if open_index == -1:
            return None
        close_index = payload.find(closing_tag, open_index)
        if close_index == -1:
            return None
        xml_bytes = payload[open_index : close_index + len(closing_tag)]
        start = close_index + len(closing_tag)
        try:
            root = ET.fromstring(xml_bytes.decode("utf-8", errors="ignore"))
        except ET.ParseError:
            continue

        fallback: Optional[str] = None
        for description in root.findall(".//dc:description", XMP_NS):
            for entry in description.findall("rdf:Alt/rdf:li", XMP_NS):
                text = normalize_extracted_text("".join(entry.itertext()))
                if text is None:
                    continue
                if entry.attrib.get("{http://www.w3.org/XML/1998/namespace}lang") == "x-default":
                    return text
                if fallback is None:
                    fallback = text
            text = normalize_extracted_text("".join(description.itertext()))
            if text is not None and fallback is None:
                fallback = text
        if fallback is not None:
            return fallback


def extract_image_metadata_from_exiftool(path: Path) -> Optional[dict[str, object]]:
    exiftool_path = shutil.which("exiftool")
    if exiftool_path is None:
        return None
    probe = subprocess.run(
        [
            exiftool_path,
            "-json",
            "-G1",
            "-a",
            "-s",
            "-XMP:all",
            "-IPTC:all",
            "-EXIF:all",
            "-JFIF:all",
            "-Photoshop:all",
            str(path),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )
    if probe.returncode != 0:
        return None
    try:
        payload = json.loads(probe.stdout)
    except json.JSONDecodeError:
        return None
    if not payload:
        return None
    raw_metadata = payload[0]
    metadata = {
        key: value
        for key, value in raw_metadata.items()
        if key != "SourceFile" and value not in (None, "")
    }
    if not metadata:
        return None
    return metadata


def extract_image_metadata(path: Path, mime: str) -> Optional[dict[str, object]]:
    if not mime.startswith("image/"):
        return None
    metadata = extract_image_metadata_from_exiftool(path)
    if metadata is not None:
        return metadata
    try:
        payload = path.read_bytes()
    except OSError:
        return None
    description = extract_xmp_description(payload)
    if description is None:
        return None
    return {"XMP-dc:Description": description}


def extractor_script_path(root: Path, extension_name: str, script_name: Path) -> Path:
    return extension_dir_path(root, extension_name) / script_name


def run_json_extractor(script_path: Path, file_path: Path, mime: str) -> Optional[object]:
    if not script_path.exists():
        return None
    probe = subprocess.run(
        [sys.executable, str(script_path), str(file_path), "--mime", mime],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if probe.returncode != 0:
        stderr = probe.stderr.strip()
        detail = f": {stderr}" if stderr else ""
        log(f"Extractor failed for {file_path}{detail}")
        return None
    payload_text = probe.stdout.strip()
    if not payload_text:
        return None
    try:
        return json.loads(payload_text)
    except json.JSONDecodeError as exc:
        log(f"Extractor returned invalid JSON for {file_path}: {exc}")
        return None


def extension_applies(extension_config: ExtensionConfig, rel_path: str, mime: str) -> bool:
    if extension_config.mime_prefixes and not mime_matches_prefixes(mime, extension_config.mime_prefixes):
        return False
    if extension_config.file_patterns and not path_matches_patterns(rel_path, extension_config.file_patterns):
        return False
    return True


def extract_extension_value(
    root: Path,
    extension_config: ExtensionConfig,
    rel_path: str,
    file_path: Path,
    mime: str,
) -> Optional[object]:
    if not extension_applies(extension_config, rel_path, mime):
        return None
    script_path = extractor_script_path(root, extension_config.name, extension_config.entrypoint)
    extracted = run_json_extractor(script_path, file_path, mime)
    if extracted is not None:
        return extracted
    if extension_config.name == "image_metadata":
        return extract_image_metadata(file_path, mime)
    return None


def normalize_git_state(status_code: str) -> str:
    if status_code == "??":
        return "untracked"
    codes = set(status_code)
    if "R" in codes:
        return "renamed"
    if "A" in codes:
        return "added"
    if "D" in codes:
        return "deleted"
    if "C" in codes:
        return "copied"
    if any(code in codes for code in ("M", "T", "U")):
        return "modified"
    return "clean"


def parse_git_status_porcelain_v2(payload: bytes) -> tuple[Optional[str], Optional[str], dict[str, GitFileStatus]]:
    branch: Optional[str] = None
    head: Optional[str] = None
    statuses: dict[str, GitFileStatus] = {}
    records = payload.split(b"\0")
    index = 0
    while index < len(records):
        raw_record = records[index]
        index += 1
        if not raw_record:
            continue
        record = raw_record.decode("utf-8", errors="replace")
        if record.startswith("# branch.head "):
            value = record.removeprefix("# branch.head ").strip()
            branch = None if value in ("(detached)", "HEAD") else value
            continue
        if record.startswith("# branch.oid "):
            value = record.removeprefix("# branch.oid ").strip()
            head = None if value == "(initial)" else value
            continue
        if record.startswith("? "):
            repo_rel_path = normalize_rel_path(record[2:])
            statuses[repo_rel_path] = GitFileStatus(repo_rel_path, "??", "untracked")
            continue
        if record.startswith("! "):
            continue
        if record.startswith("1 "):
            parts = record.split(" ", 8)
            if len(parts) != 9:
                continue
            status_code = parts[1]
            repo_rel_path = normalize_rel_path(parts[8])
        elif record.startswith("2 "):
            parts = record.split(" ", 9)
            if len(parts) != 10:
                continue
            status_code = parts[1]
            repo_rel_path = normalize_rel_path(parts[9])
            if index < len(records):
                index += 1
        elif record.startswith("u "):
            parts = record.split(" ", 10)
            if len(parts) != 11:
                continue
            status_code = parts[1]
            repo_rel_path = normalize_rel_path(parts[10])
        else:
            continue
        statuses[repo_rel_path] = GitFileStatus(repo_rel_path, status_code, normalize_git_state(status_code))
    return branch, head, statuses


def capture_git_scan_context(root: Path, repo_root: Path) -> GitScanContext:
    repo_probe = subprocess.run(
        ["git", "-C", str(repo_root), "rev-parse", "--show-toplevel"],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )
    if repo_probe.returncode != 0:
        return GitScanContext(False, None, None, None, None, {})

    repo_root_abs = Path(repo_probe.stdout.strip()).resolve()
    requested_root_abs = repo_root.resolve()
    if repo_root_abs != requested_root_abs:
        return GitScanContext(False, None, None, None, None, {})
    repo_root_rel = normalize_rel_path(os.path.relpath(repo_root_abs, root))
    status_probe = subprocess.run(
        ["git", "-C", str(repo_root), "status", "--porcelain=2", "--branch", "-z", "--untracked-files=all"],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=True,
    )
    git_branch, git_head, statuses = parse_git_status_porcelain_v2(status_probe.stdout)
    return GitScanContext(True, repo_root_abs, repo_root_rel, git_branch, git_head, statuses)


def ensure_blob_preserved(conn: sqlite3.Connection, root: Path, file_path: Path, blob_hash: str, size_bytes: int, tx_id: int) -> None:
    row = conn.execute("SELECT blob_hash FROM blob_object WHERE blob_hash = ?", (blob_hash,)).fetchone()
    if row is not None:
        return
    rel = f"{blob_hash[:2]}/{blob_hash}"
    blob_path = store_root(root) / rel
    blob_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(file_path, blob_path)
    conn.execute(
        """
        INSERT INTO blob_object (blob_hash, algo, size_bytes, storage_relpath, created_tx_id)
        VALUES (?, 'sha256', ?, ?, ?)
        """,
        (blob_hash, size_bytes, f".sysstore/objects/{rel}", tx_id),
    )
    log(f"Preserved new blob {blob_hash[:12]} from {file_path}")


def ensure_file_entity(conn: sqlite3.Connection, rel_path: str, tx_id: int) -> int:
    stable_key = f"file:{rel_path}"
    row = conn.execute(
        "SELECT entity_id FROM entity WHERE entity_type = 'file' AND stable_key = ?",
        (stable_key,),
    ).fetchone()
    if row is not None:
        return int(row["entity_id"])

    cur = conn.execute(
        "INSERT INTO entity (entity_type, stable_key, created_tx_id) VALUES ('file', ?, ?)",
        (stable_key, tx_id),
    )
    entity_id = int(cur.lastrowid)
    canonical_uri = rel_path
    conn.execute(
        """
        INSERT INTO file_entry (
            file_id, canonical_uri, first_seen_tx_id,
            current_path, current_name, current_extension,
            current_mime, current_kind, current_hash,
            current_size_bytes, current_mtime
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            entity_id,
            canonical_uri,
            tx_id,
            rel_path,
            Path(rel_path).name,
            Path(rel_path).suffix.lower(),
            None,
            None,
            None,
            None,
            None,
        ),
    )
    log(f"Created file entity {entity_id} for {rel_path}")
    return entity_id


def create_scan_run(conn: sqlite3.Connection, scan_root: str, git_ctx: GitScanContext) -> int:
    cur = conn.execute(
        """
        INSERT INTO scan_run (
            scan_time,
            scan_root,
            is_git_repo,
            git_repo_root,
            git_branch,
            git_head
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            utc_now(),
            scan_root or ".",
            int(git_ctx.is_git_repo),
            git_ctx.git_repo_root,
            git_ctx.git_branch,
            git_ctx.git_head,
        ),
    )
    return int(cur.lastrowid)


def append_fact(conn: sqlite3.Connection, tx_id: int, entity_id: int, attr_ident: str, value, added: int = 1) -> None:
    attr_id, value_type = get_attr_info(conn, attr_ident)
    if value_type == "json":
        payload = value if isinstance(value, str) else json.dumps(value, sort_keys=True)
        conn.execute(
            "INSERT INTO fact (tx_id, entity_id, attr_id, value_json, added) VALUES (?, ?, ?, ?, ?)",
            (tx_id, entity_id, attr_id, payload, added),
        )
    elif isinstance(value, bool):
        conn.execute(
            "INSERT INTO fact (tx_id, entity_id, attr_id, value_bool, added) VALUES (?, ?, ?, ?, ?)",
            (tx_id, entity_id, attr_id, int(value), added),
        )
    elif isinstance(value, int):
        conn.execute(
            "INSERT INTO fact (tx_id, entity_id, attr_id, value_int, added) VALUES (?, ?, ?, ?, ?)",
            (tx_id, entity_id, attr_id, value, added),
        )
    elif value_type == "blobref":
        conn.execute(
            "INSERT INTO fact (tx_id, entity_id, attr_id, value_blobref, added) VALUES (?, ?, ?, ?, ?)",
            (tx_id, entity_id, attr_id, str(value), added),
        )
    else:
        conn.execute(
            "INSERT INTO fact (tx_id, entity_id, attr_id, value_text, added) VALUES (?, ?, ?, ?, ?)",
            (tx_id, entity_id, attr_id, str(value), added),
        )


def update_projection(
    conn: sqlite3.Connection,
    entity_id: int,
    rel_path: str,
    mime: str,
    kind: str,
    blob_hash: str,
    size_bytes: int,
    mtime: str,
) -> None:
    conn.execute(
        """
        UPDATE file_entry
        SET current_path = ?,
            current_name = ?,
            current_extension = ?,
            current_mime = ?,
            current_kind = ?,
            current_hash = ?,
            current_size_bytes = ?,
            current_mtime = ?,
            is_deleted = 0
        WHERE file_id = ?
        """,
        (
            rel_path,
            Path(rel_path).name,
            Path(rel_path).suffix.lower(),
            mime,
            kind,
            blob_hash,
            size_bytes,
            mtime,
            entity_id,
        ),
    )


def append_file_scan_git(
    conn: sqlite3.Connection,
    scan_id: int,
    entity_id: int,
    git_repo_root: Optional[str],
    git_branch: Optional[str],
    git_head: Optional[str],
    repo_rel_path: str,
    git_status_raw: str,
    git_state: str,
) -> None:
    conn.execute(
        """
        INSERT INTO file_scan_git (
            scan_id,
            file_id,
            git_repo_root,
            git_branch,
            git_head,
            repo_rel_path,
            git_status_raw,
            git_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (scan_id, entity_id, git_repo_root, git_branch, git_head, repo_rel_path, git_status_raw, git_state),
    )


def decode_json_value(value: Optional[str]):
    if value is None:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def row_value(row: sqlite3.Row, decode_json: bool = False):
    value = row["value_text"]
    if value is not None:
        return value
    value = row["value_int"]
    if value is not None:
        return value
    value = row["value_blobref"]
    if value is not None:
        return value
    value = row["value_json"]
    if decode_json:
        return decode_json_value(value)
    return value


def render_row_value(row: sqlite3.Row) -> str:
    value = row_value(row, decode_json=True)
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return str(value)


def iso_mtime(stat_result: os.stat_result) -> str:
    return dt.datetime.fromtimestamp(stat_result.st_mtime, tz=dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def path_is_within(path: Path, ancestor: Path) -> bool:
    try:
        path.relative_to(ancestor)
        return True
    except ValueError:
        return False


def has_local_git_entry(path: Path) -> bool:
    git_entry = path / ".git"
    return git_entry.exists()


def resolve_active_git_root(
    current_dir: Path,
    scan_root: Path,
    repo_roots: set[Path],
    active_repo_cache: dict[Path, Optional[Path]],
) -> Optional[Path]:
    cached = active_repo_cache.get(current_dir)
    if cached is not None or current_dir in active_repo_cache:
        return cached
    probe = current_dir
    while path_is_within(probe, scan_root):
        if probe in repo_roots:
            active_repo_cache[current_dir] = probe
            return probe
        if probe == scan_root:
            break
        probe = probe.parent
    active_repo_cache[current_dir] = None
    return None


def scan_repo(root: Path, scan_root: Path, actor: str, extract_meta_flag: bool) -> None:
    ensure_repo_exists(root)
    enabled_extensions = load_enabled_extensions(root)
    if extract_meta_flag:
        log(f"--extract-meta is deprecated; configure {EXTENSIONS_FILE} instead")
    patterns = load_ignore_patterns(root)
    conn = connect_db(root)
    seed_attributes(conn, 0)
    ensure_extension_attributes(conn, enabled_extensions)
    scanned = 0
    skipped = 0
    scan_root = scan_root.resolve()
    git_ctx_cache: dict[Path, GitScanContext] = {}
    discovered_repo_roots: set[Path] = set()
    active_repo_cache: dict[Path, Optional[Path]] = {}
    scan_git_ctx = GitScanContext(False, None, None, None, None, {})
    if has_local_git_entry(scan_root):
        scan_git_ctx = capture_git_scan_context(root, scan_root)
        if scan_git_ctx.is_git_repo and scan_git_ctx.repo_root_abs is not None:
            git_ctx_cache[scan_git_ctx.repo_root_abs] = scan_git_ctx
            discovered_repo_roots.add(scan_git_ctx.repo_root_abs)
    scan_root_rel = normalize_rel_path(os.path.relpath(scan_root, root))
    try:
        scan_id = create_scan_run(conn, scan_root_rel, scan_git_ctx)
        if scan_git_ctx.is_git_repo:
            branch_label = scan_git_ctx.git_branch or "(detached)"
            repo_label = scan_git_ctx.git_repo_root or "."
            log(f"Git scan context: repo={repo_label} branch={branch_label}")
        for dirpath, dirnames, filenames in os.walk(scan_root):
            current_dir = Path(dirpath).resolve()
            dirnames.sort()
            filenames.sort()

            kept_dirnames: list[str] = []
            for dirname in dirnames:
                child_dir = current_dir / dirname
                child_rel = normalize_rel_path(os.path.relpath(child_dir, root))
                if is_ignored(child_rel, patterns):
                    skipped += 1
                    log(f"Ignoring {child_rel}")
                    continue
                kept_dirnames.append(dirname)
            dirnames[:] = kept_dirnames

            if has_local_git_entry(current_dir) and current_dir not in git_ctx_cache:
                git_ctx = capture_git_scan_context(root, current_dir)
                if git_ctx.is_git_repo and git_ctx.repo_root_abs is not None:
                    git_ctx_cache[git_ctx.repo_root_abs] = git_ctx
                    discovered_repo_roots.add(git_ctx.repo_root_abs)
                    branch_label = git_ctx.git_branch or "(detached)"
                    repo_label = git_ctx.git_repo_root or "."
                    log(f"Discovered nested git repo: repo={repo_label} branch={branch_label}")

            active_repo_root = resolve_active_git_root(current_dir, scan_root, discovered_repo_roots, active_repo_cache)
            active_git_ctx = git_ctx_cache.get(active_repo_root) if active_repo_root is not None else None

            for filename in filenames:
                file_path = current_dir / filename
                rel_to_repo = normalize_rel_path(os.path.relpath(file_path, root))
                if is_ignored(rel_to_repo, patterns):
                    skipped += 1
                    log(f"Ignoring {rel_to_repo}")
                    continue
                if not file_path.is_file():
                    skipped += 1
                    continue
                stat_result = file_path.stat()
                size_bytes = int(stat_result.st_size)
                mtime = iso_mtime(stat_result)
                mime = detect_mime(file_path)
                kind = classify_kind(mime)
                blob_hash = sha256_file(file_path)
                tx_id = create_tx(conn, actor=actor, source="scan", message=f"scan {rel_to_repo}")
                entity_id = ensure_file_entity(conn, rel_to_repo, tx_id)
                ensure_blob_preserved(conn, root, file_path, blob_hash, size_bytes, tx_id)
                append_fact(conn, tx_id, entity_id, "fs/path", rel_to_repo)
                append_fact(conn, tx_id, entity_id, "fs/name", Path(rel_to_repo).name)
                append_fact(conn, tx_id, entity_id, "fs/extension", Path(rel_to_repo).suffix.lower())
                append_fact(conn, tx_id, entity_id, "fs/mime", mime)
                append_fact(conn, tx_id, entity_id, "fs/kind", kind)
                append_fact(conn, tx_id, entity_id, "fs/hash", blob_hash)
                append_fact(conn, tx_id, entity_id, "fs/size_bytes", size_bytes)
                append_fact(conn, tx_id, entity_id, "fs/mtime", mtime)
                append_fact(conn, tx_id, entity_id, "fs/blob_hash", blob_hash)
                for extension_config in enabled_extensions:
                    extracted = extract_extension_value(
                        root,
                        extension_config,
                        rel_to_repo,
                        file_path,
                        mime,
                    )
                    if extracted is not None:
                        append_fact(conn, tx_id, entity_id, extension_config.attr_ident, extracted)
                update_projection(conn, entity_id, rel_to_repo, mime, kind, blob_hash, size_bytes, mtime)
                if active_git_ctx is not None and active_git_ctx.repo_root_abs is not None:
                    repo_rel_path = normalize_rel_path(os.path.relpath(file_path, active_git_ctx.repo_root_abs))
                    git_status = active_git_ctx.statuses.get(repo_rel_path)
                    if git_status is None:
                        git_status = GitFileStatus(repo_rel_path, "", "clean")
                    append_file_scan_git(
                        conn,
                        scan_id,
                        entity_id,
                        active_git_ctx.git_repo_root,
                        active_git_ctx.git_branch,
                        active_git_ctx.git_head,
                        repo_rel_path,
                        git_status.git_status_raw,
                        git_status.git_state,
                    )
                scanned += 1
        conn.commit()
    finally:
        conn.close()
    log(f"Scan complete: scanned={scanned} skipped={skipped}")


def list_files(root: Path, as_json: bool) -> None:
    ensure_repo_exists(root)
    conn = connect_db(root)
    try:
        rows = conn.execute(
            """
            SELECT file_id, current_path, current_mime, current_kind
            FROM file_entry
            WHERE is_deleted = 0
            ORDER BY file_id
            """
        ).fetchall()
    finally:
        conn.close()

    if as_json:
        payload = [dict(row) for row in rows]
        json.dump(payload, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return

    for row in rows:
        sys.stdout.write(
            f"{row['file_id']}\t{row['current_path']}\t{row['current_mime']}\t{row['current_kind']}\n"
        )


def show_history(root: Path, entity_id: int, as_json: bool) -> None:
    ensure_repo_exists(root)
    conn = connect_db(root)
    try:
        rows = conn.execute(
            """
            SELECT
                t.tx_time,
                a.ident,
                f.added,
                f.value_text,
                f.value_int,
                f.value_blobref,
                f.value_json
            FROM fact f
            JOIN tx t ON t.tx_id = f.tx_id
            JOIN attribute a ON a.attr_id = f.attr_id
            WHERE f.entity_id = ?
            ORDER BY t.tx_time, f.fact_id
            """,
            (entity_id,),
        ).fetchall()
    finally:
        conn.close()

    if as_json:
        payload = []
        for row in rows:
            item = dict(row)
            if item["value_json"] is not None:
                item["value_json"] = decode_json_value(item["value_json"])
            payload.append(item)
        json.dump(payload, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return

    for row in rows:
        sign = "+" if row["added"] == 1 else "-"
        sys.stdout.write(f"{row['tx_time']}\t{sign}\t{row['ident']}\t{render_row_value(row)}\n")


def retract_fact(
    root: Path,
    entity_id: int,
    attr_ident: str,
    value_text: Optional[str],
    value_int: Optional[int],
    value_blobref: Optional[str],
    value_json: Optional[str],
    actor: str,
) -> None:
    ensure_repo_exists(root)
    conn = connect_db(root)
    try:
        tx_id = create_tx(conn, actor=actor, source="retract", message=f"retract {attr_ident} on entity {entity_id}")
        attr_id = get_attr_id(conn, attr_ident)
        provided = [value_text is not None, value_int is not None, value_blobref is not None, value_json is not None]
        if sum(provided) != 1:
            raise SystemExit("Provide exactly one of --value-text, --value-int, --value-blobref, or --value-json")
        if value_text is not None:
            conn.execute(
                "INSERT INTO fact (tx_id, entity_id, attr_id, value_text, added) VALUES (?, ?, ?, ?, 0)",
                (tx_id, entity_id, attr_id, value_text),
            )
        elif value_int is not None:
            conn.execute(
                "INSERT INTO fact (tx_id, entity_id, attr_id, value_int, added) VALUES (?, ?, ?, ?, 0)",
                (tx_id, entity_id, attr_id, value_int),
            )
        elif value_json is not None:
            conn.execute(
                "INSERT INTO fact (tx_id, entity_id, attr_id, value_json, added) VALUES (?, ?, ?, ?, 0)",
                (tx_id, entity_id, attr_id, json.dumps(json.loads(value_json), sort_keys=True)),
            )
        else:
            conn.execute(
                "INSERT INTO fact (tx_id, entity_id, attr_id, value_blobref, added) VALUES (?, ?, ?, ?, 0)",
                (tx_id, entity_id, attr_id, value_blobref),
            )
        conn.commit()
    finally:
        conn.close()
    log(f"Retracted fact for entity={entity_id} attr={attr_ident}")


def forget_root(root: Path, scope_value: str) -> None:
    ensure_repo_exists(root)
    scope = normalize_scope_arg(root, scope_value)
    conn = connect_db(root)
    conn.execute("BEGIN")
    try:
        file_rows = conn.execute(
            """
            SELECT file_id, COALESCE(current_path, canonical_uri) AS path_value
            FROM file_entry
            """
        ).fetchall()
        file_ids = [
            int(row["file_id"])
            for row in file_rows
            if scope_matches_path(str(row["path_value"] or ""), scope)
        ]

        scan_rows = conn.execute("SELECT scan_id, scan_root FROM scan_run").fetchall()
        scan_ids = [
            int(row["scan_id"])
            for row in scan_rows
            if scope_matches_path(str(row["scan_root"] or ""), scope)
        ]

        removed_file_scan_git = 0
        removed_facts = 0
        removed_files = 0
        if file_ids:
            file_placeholders = ",".join("?" for _ in file_ids)
            removed_file_scan_git += conn.execute(
                f"DELETE FROM file_scan_git WHERE file_id IN ({file_placeholders})",
                tuple(file_ids),
            ).rowcount
            removed_facts = conn.execute(
                f"DELETE FROM fact WHERE entity_id IN ({file_placeholders})",
                tuple(file_ids),
            ).rowcount
            removed_files = conn.execute(
                f"DELETE FROM file_entry WHERE file_id IN ({file_placeholders})",
                tuple(file_ids),
            ).rowcount
            conn.execute(
                f"DELETE FROM entity WHERE entity_id IN ({file_placeholders})",
                tuple(file_ids),
            )

        removed_scan_runs = 0
        if scan_ids:
            scan_placeholders = ",".join("?" for _ in scan_ids)
            removed_file_scan_git += conn.execute(
                f"DELETE FROM file_scan_git WHERE scan_id IN ({scan_placeholders})",
                tuple(scan_ids),
            ).rowcount
            removed_scan_runs = conn.execute(
                f"DELETE FROM scan_run WHERE scan_id IN ({scan_placeholders})",
                tuple(scan_ids),
            ).rowcount

        orphan_blob_rows = conn.execute(
            """
            SELECT blob_hash, storage_relpath
            FROM blob_object
            WHERE NOT EXISTS (
                SELECT 1
                FROM fact
                WHERE value_blobref = blob_object.blob_hash
            )
            """
        ).fetchall()
        orphan_blob_hashes = [str(row["blob_hash"]) for row in orphan_blob_rows]
        orphan_blob_paths = [root / str(row["storage_relpath"]) for row in orphan_blob_rows]
        removed_blobs = 0
        if orphan_blob_hashes:
            blob_placeholders = ",".join("?" for _ in orphan_blob_hashes)
            removed_blobs = conn.execute(
                f"DELETE FROM blob_object WHERE blob_hash IN ({blob_placeholders})",
                tuple(orphan_blob_hashes),
            ).rowcount

        removed_tx = conn.execute(
            """
            DELETE FROM tx
            WHERE source = 'scan'
              AND NOT EXISTS (SELECT 1 FROM fact WHERE fact.tx_id = tx.tx_id)
              AND NOT EXISTS (SELECT 1 FROM entity WHERE entity.created_tx_id = tx.tx_id)
              AND NOT EXISTS (SELECT 1 FROM blob_object WHERE blob_object.created_tx_id = tx.tx_id)
            """
        ).rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    removed_blob_files = 0
    for blob_path in orphan_blob_paths:
        try:
            blob_path.unlink()
            removed_blob_files += 1
        except FileNotFoundError:
            continue

    for blob_path in orphan_blob_paths:
        parent = blob_path.parent
        while parent != root:
            try:
                parent.rmdir()
            except OSError:
                break
            parent = parent.parent

    scope_label = scope or "."
    log(
        "Forget complete: "
        f"scope={scope_label} files={removed_files} facts={removed_facts} "
        f"file_scan_git={removed_file_scan_git} scans={removed_scan_runs} "
        f"blobs={removed_blobs} blob_files={removed_blob_files} tx={removed_tx}"
    )


def as_of(root: Path, entity_id: int, time_value: str, as_json: bool) -> None:
    ensure_repo_exists(root)
    conn = connect_db(root)
    try:
        rows = conn.execute(
            """
            WITH ranked AS (
                SELECT
                    a.ident,
                    f.added,
                    f.value_text,
                    f.value_int,
                    f.value_json,
                    f.value_blobref,
                    ROW_NUMBER() OVER (
                        PARTITION BY f.entity_id, f.attr_id
                        ORDER BY f.tx_id DESC, f.fact_id DESC
                    ) AS rn
                FROM fact f
                JOIN tx t ON t.tx_id = f.tx_id
                JOIN attribute a ON a.attr_id = f.attr_id
                WHERE f.entity_id = ?
                  AND t.tx_time <= ?
            )
            SELECT ident, value_text, value_int, value_json, value_blobref
            FROM ranked
            WHERE rn = 1 AND added = 1
            ORDER BY ident
            """,
            (entity_id, time_value),
        ).fetchall()
    finally:
        conn.close()

    if as_json:
        payload = []
        for row in rows:
            item = dict(row)
            if item["value_json"] is not None:
                item["value_json"] = decode_json_value(item["value_json"])
            payload.append(item)
        json.dump(payload, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return

    for row in rows:
        sys.stdout.write(f"{row['ident']}\t{render_row_value(row)}\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Local-first metadata tracker with immutable facts over time")
    parser.add_argument("--repo", default=".", help="Repository root containing .sysmvp.db")
    parser.add_argument("--actor", default=os.environ.get("USER", "unknown"), help="Actor recorded in transactions")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="Initialize repository")

    scan_parser = sub.add_parser("scan", help="Scan files and append facts")
    scan_parser.add_argument("--root", default=".", help="Filesystem root to scan")
    scan_parser.add_argument(
        "--extract-meta",
        action="store_true",
        help=f"Deprecated compatibility flag; extensions are configured in {EXTENSIONS_FILE}",
    )

    list_parser = sub.add_parser("list", help="List current tracked files")
    list_parser.add_argument("--json", action="store_true", help="Emit JSON")

    hist_parser = sub.add_parser("history", help="Show entity history")
    hist_parser.add_argument("entity_id", type=int)
    hist_parser.add_argument("--json", action="store_true", help="Emit JSON")

    retract_parser = sub.add_parser("retract", help="Append a retraction fact")
    retract_parser.add_argument("entity_id", type=int)
    retract_parser.add_argument("attr_ident")
    retract_parser.add_argument("--value-text")
    retract_parser.add_argument("--value-int", type=int)
    retract_parser.add_argument("--value-blobref")
    retract_parser.add_argument("--value-json")

    asof_parser = sub.add_parser("as-of", help="Show latest active facts as of a time")
    asof_parser.add_argument("entity_id", type=int)
    asof_parser.add_argument("--time", required=True, help="UTC timestamp, e.g. 2026-04-21T12:00:00Z")
    asof_parser.add_argument("--json", action="store_true", help="Emit JSON")

    forget_parser = sub.add_parser("forget-root", help="Purge one scanned directory scope from the repository")
    forget_parser.add_argument("scope", help="Repo-relative or absolute scan root to forget")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    root = repo_root_from(Path(args.repo))

    if args.command == "init":
        init_repo(root)
        return 0
    if args.command == "scan":
        scan_repo(root, Path(args.root).resolve(), args.actor, args.extract_meta)
        return 0
    if args.command == "list":
        list_files(root, args.json)
        return 0
    if args.command == "history":
        show_history(root, args.entity_id, args.json)
        return 0
    if args.command == "retract":
        retract_fact(
            root,
            args.entity_id,
            args.attr_ident,
            args.value_text,
            args.value_int,
            args.value_blobref,
            args.value_json,
            args.actor,
        )
        return 0
    if args.command == "as-of":
        as_of(root, args.entity_id, args.time, args.json)
        return 0
    if args.command == "forget-root":
        forget_root(root, args.scope)
        return 0

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
