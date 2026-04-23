# Source Control Under Management

A local-first metadata tracker for files using **SQLite for metadata only** and a **copy-on-write object store** on disk for preserved file contents.

This MVP is deliberately small. It is not pretending to be a full source-control system. It shows the core shape:

- immutable facts appended over time
- explicit transactions
- stable entity IDs
- copy-on-write preserved blobs outside SQLite
- `.sysignore` support for skipping files and directories
- current-state projection for fast reads

## Principles

### Facts over time
The database stores changes as appended facts instead of mutating history in place.

### Metadata in SQLite, bytes on disk
SQLite stores metadata only. Preserved file contents are copied into `.sysstore/objects/...` using content hashes.

### Stable identity
A tracked file gets a stable entity ID. Paths are observed facts that can change over time.

### Explicit change recording
The system appends:
- assertions when it observes current state
- retractions when you explicitly retract values

### Read optimization without rewriting history
The `file_entry` table is a current-state projection for fast listing. The `fact` table remains the historical record.

### Local-first
No server. No daemon. No extra infrastructure.

## What the MVP does

- initializes a repository under a directory
- creates `.sysmvp.db`
- creates `.sysstore/objects/` for preserved content
- reads `.sysignore`
- scans files under the current directory
- ignores excluded paths
- hashes file contents
- preserves unseen blobs by content hash
- appends facts for path, hash, size, mtime, blob hash, mime, kind
- updates a current-state projection table
- shows current files
- shows per-file history
- supports explicit fact retraction

## Repository layout

```text
.
├── .sysextensions.json
├── .sysignore
├── .sysmvp.db
├── .sysstore/
│   └── objects/
│       └── ab/
│           └── abcdef...
├── sysmvp.py
├── schema.sql
└── justfile
```

## Commands

### Initialize

```bash
python3 sysmvp.py init
```

This creates:

- `.sysmvp.db`
- `.sysstore/objects/`
- a default `.sysignore` if one does not already exist
- a default `.sysextensions.json` if one does not already exist

### Scan current directory

```bash
python3 sysmvp.py scan
```

You can also scan another root while storing metadata in the current repo:

```bash
python3 sysmvp.py scan --root ./examples/demo
```

Extensions are configured in `.sysextensions.json`. To enable image metadata
extraction, set:

```json
{
  "extensions": {
    "asciidoc_header": {
      "enabled": true
    },
    "image_metadata": {
      "enabled": true
    }
  }
}
```

Then run a normal scan:

```bash
python3 sysmvp.py scan --root ./examples/demo
```

When `image_metadata` is enabled, `sysmvp.py` first tries the repo-local
extension script at `extractors/image_metadata/run.py`. Each extension can live
in its own subdirectory with local docs such as
`extractors/image_metadata/image_metadata.md` and defaults in
`extractors/image_metadata/extension.json`. Repo config may optionally override
defaults such as `file_patterns` or `mime_prefixes` in `.sysextensions.json`.
If the image metadata script is missing, fails, or returns invalid JSON, the
scan falls back to the built-in image metadata parser so scans stay resilient.

When `asciidoc_header` is enabled, `sysmvp.py` runs
`extractors/asciidoc_header/run.py` for matching `.adoc` files and writes the
resulting JSON fact to `asciidoc/header`.

### List current tracked files

```bash
python3 sysmvp.py list
```

Default output is tab-separated text to stdout.

JSON output:

```bash
python3 sysmvp.py list --json
```

### Browse the database in a browser

```bash
python3 sysbrowse.py
```

Then open `http://127.0.0.1:8000`.

The browser UI provides:

- a files view backed by `file_entry`
- a blobs view backed by `blob_object`
- a transactions view backed by `tx`
- a global repo-path prefix filter such as `examples/demo`
- stats that follow the active path filter
- version history for a file with links to older preserved blobs
- file detail with immutable fact history
- direct links to preserved blob bytes under `.sysstore/objects/...`

### Show history for one entity

```bash
python3 sysmvp.py history 1
```

### Retract an exact value

Retract a tag-like or exact value fact:

```bash
python3 sysmvp.py retract 1 fs/path --value-text ./old/path.txt
```

This does not delete history. It appends a retraction fact.

### Forget a scanned directory

Purge one scanned directory scope from the repository, including matching current files,
facts, git scan rows, and unreferenced preserved blobs:

```bash
python3 sysmvp.py forget-root examples/demo
```

This is a destructive cleanup for one scan scope. Use `.sysignore` if you only want to
stop future scans from ingesting that directory.

### Show state as of a timestamp

```bash
python3 sysmvp.py as-of 1 --time 2026-04-21T12:00:00Z
```

## Output conventions

- main results go to stdout
- logs and debug messages go to stderr

That makes it easy to pipe results:

```bash
python3 sysmvp.py list --json > files.json
python3 sysmvp.py history 1 2> debug.log
```

## Ignore file

The default `.sysignore` uses gitignore-like simple patterns.

Examples:

```gitignore
.git/
node_modules/
*.log
.sysstore/
```

Supported in this MVP:

- exact filename matches
- directory suffix matches like `.git/`
- shell-style globs via `fnmatch`

## Schema summary

### `tx`
A transaction log with timestamp and message.

### `entity`
Stable IDs for tracked things.

### `attribute`
Registry of known attributes.

### `fact`
Immutable asserted or retracted facts.

### `blob_object`
Metadata for preserved content-addressed blobs.

### `file_entry`
Current-state projection for fast reads.

## Usage example

```bash
python3 sysmvp.py init
mkdir -p examples/demo
printf 'hello\n' > examples/demo/a.txt
printf 'world\n' > examples/demo/b.txt
python3 sysmvp.py scan --root examples/demo
python3 sysmvp.py list
python3 sysmvp.py history 1
```

## just targets

```bash
just init
just demo
just serve
just list
just history 1
```

## MVP limitations

This is intentionally narrow.

- no file move detection beyond observed path changes
- no rename inference
- no symlink handling
- no directory entities yet
- no extension-specific settings beyond enabled or disabled yet
- no tags or notes commands yet
- no parallel hashing yet
- no live watch mode yet

## Extending it

Good next steps:

- add `file_version` entities
- add tag and note commands
- add extractors for image, PDF, code, audio, video
- add derived artifacts like thumbnails or extracted text
- add current-state materialized tables for hot queries
- add tests around as-of semantics and retractions

## Development

Run a small demo:

```bash
just demo
```

Run a tiny smoke test:

```bash
just test
```

This runs the base scan smoke test, the dedicated image metadata extension smoke
test, and the browser smoke test.
