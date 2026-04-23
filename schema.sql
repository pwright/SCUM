PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS tx (
    tx_id            INTEGER PRIMARY KEY,
    tx_time          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    actor            TEXT,
    source           TEXT,
    message          TEXT
);

CREATE INDEX IF NOT EXISTS idx_tx_time ON tx(tx_time);

CREATE TABLE IF NOT EXISTS entity (
    entity_id         INTEGER PRIMARY KEY,
    entity_type       TEXT NOT NULL,
    stable_key        TEXT,
    created_tx_id     INTEGER NOT NULL REFERENCES tx(tx_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_entity_stable_key
    ON entity(entity_type, stable_key)
    WHERE stable_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS attribute (
    attr_id               INTEGER PRIMARY KEY,
    ident                 TEXT NOT NULL UNIQUE,
    value_type            TEXT NOT NULL CHECK (
        value_type IN ('text','int','real','bool','json','ref','blobref')
    ),
    cardinality           TEXT NOT NULL CHECK (
        cardinality IN ('one','many')
    ),
    description           TEXT
);

CREATE TABLE IF NOT EXISTS fact (
    fact_id               INTEGER PRIMARY KEY,
    tx_id                 INTEGER NOT NULL REFERENCES tx(tx_id),
    entity_id             INTEGER NOT NULL REFERENCES entity(entity_id),
    attr_id               INTEGER NOT NULL REFERENCES attribute(attr_id),

    value_text            TEXT,
    value_int             INTEGER,
    value_real            REAL,
    value_bool            INTEGER CHECK (value_bool IN (0,1)),
    value_json            TEXT,
    value_ref             INTEGER REFERENCES entity(entity_id),
    value_blobref         TEXT,

    added                 INTEGER NOT NULL CHECK (added IN (0,1)),

    CHECK (
        (value_text IS NOT NULL) +
        (value_int IS NOT NULL) +
        (value_real IS NOT NULL) +
        (value_bool IS NOT NULL) +
        (value_json IS NOT NULL) +
        (value_ref IS NOT NULL) +
        (value_blobref IS NOT NULL)
        = 1
    )
);

CREATE INDEX IF NOT EXISTS idx_fact_entity_attr_tx
    ON fact(entity_id, attr_id, tx_id, fact_id);
CREATE INDEX IF NOT EXISTS idx_fact_attr_text
    ON fact(attr_id, value_text, tx_id)
    WHERE value_text IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_fact_attr_int
    ON fact(attr_id, value_int, tx_id)
    WHERE value_int IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_fact_attr_blobref
    ON fact(attr_id, value_blobref, tx_id)
    WHERE value_blobref IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_fact_tx ON fact(tx_id);

CREATE TABLE IF NOT EXISTS blob_object (
    blob_hash             TEXT PRIMARY KEY,
    algo                  TEXT NOT NULL DEFAULT 'sha256',
    size_bytes            INTEGER NOT NULL,
    storage_relpath       TEXT NOT NULL UNIQUE,
    created_tx_id         INTEGER NOT NULL REFERENCES tx(tx_id)
);

CREATE TABLE IF NOT EXISTS scan_run (
    scan_id               INTEGER PRIMARY KEY,
    scan_time             TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    scan_root             TEXT NOT NULL,
    is_git_repo           INTEGER NOT NULL CHECK (is_git_repo IN (0,1)),
    git_repo_root         TEXT,
    git_branch            TEXT,
    git_head              TEXT
);

CREATE INDEX IF NOT EXISTS idx_scan_run_repo_branch_time
    ON scan_run(git_repo_root, git_branch, scan_time);

CREATE TABLE IF NOT EXISTS file_scan_git (
    scan_id               INTEGER NOT NULL REFERENCES scan_run(scan_id),
    file_id               INTEGER NOT NULL REFERENCES entity(entity_id),
    git_repo_root         TEXT,
    git_branch            TEXT,
    git_head              TEXT,
    repo_rel_path         TEXT NOT NULL,
    git_status_raw        TEXT,
    git_state             TEXT NOT NULL,
    PRIMARY KEY (scan_id, file_id)
);

CREATE INDEX IF NOT EXISTS idx_file_scan_git_scan_state
    ON file_scan_git(scan_id, git_state);

CREATE INDEX IF NOT EXISTS idx_file_scan_git_file_scan
    ON file_scan_git(file_id, scan_id);

CREATE INDEX IF NOT EXISTS idx_file_scan_git_repo_branch_scan
    ON file_scan_git(git_repo_root, git_branch, scan_id);

CREATE TABLE IF NOT EXISTS file_entry (
    file_id                INTEGER PRIMARY KEY REFERENCES entity(entity_id),
    canonical_uri          TEXT NOT NULL UNIQUE,
    first_seen_tx_id       INTEGER NOT NULL REFERENCES tx(tx_id),

    current_path           TEXT,
    current_name           TEXT,
    current_extension      TEXT,
    current_mime           TEXT,
    current_kind           TEXT,
    current_hash           TEXT,
    current_size_bytes     INTEGER,
    current_mtime          TEXT,

    is_deleted             INTEGER NOT NULL DEFAULT 0 CHECK (is_deleted IN (0,1))
);

CREATE INDEX IF NOT EXISTS idx_file_entry_kind ON file_entry(current_kind);
CREATE INDEX IF NOT EXISTS idx_file_entry_mime ON file_entry(current_mime);
CREATE INDEX IF NOT EXISTS idx_file_entry_hash ON file_entry(current_hash);
CREATE INDEX IF NOT EXISTS idx_file_entry_ext ON file_entry(current_extension);

CREATE VIEW IF NOT EXISTS v_current_fact AS
WITH ranked AS (
    SELECT
        f.*,
        ROW_NUMBER() OVER (
            PARTITION BY f.entity_id, f.attr_id
            ORDER BY f.tx_id DESC, f.fact_id DESC
        ) AS rn
    FROM fact f
)
SELECT *
FROM ranked
WHERE rn = 1
  AND added = 1;
