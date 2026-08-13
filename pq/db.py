"""SQLite queue database: schema, migrations, connection helper."""
from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_VERSION = 1

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_name TEXT NOT NULL,
    pipeline_dir TEXT NOT NULL,
    inputs_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('queued','waiting','running','done','failed','cancelled')),
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    cooldown_until TEXT,
    error TEXT
);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_cooldown ON runs(cooldown_until);

CREATE TABLE IF NOT EXISTS steps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    step_id TEXT NOT NULL,
    needs_json TEXT NOT NULL DEFAULT '[]',
    iterates_json TEXT,
    produces_json TEXT,
    type TEXT,
    status TEXT NOT NULL CHECK (status IN ('pending','running','done','failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    UNIQUE(run_id, step_id)
);

CREATE TABLE IF NOT EXISTS step_iterations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    step_run_id INTEGER NOT NULL REFERENCES steps(id) ON DELETE CASCADE,
    iteration INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending','running','done','skipped','failed')),
    log_path TEXT,
    exit_code INTEGER,
    UNIQUE(step_run_id, iteration)
);

CREATE TABLE IF NOT EXISTS counters (
    day TEXT PRIMARY KEY,
    uploads_count INTEGER NOT NULL DEFAULT 0
);
"""


def init_db(data_dir: Path) -> Path:
    """Ensure the data dir exists, create pq.db with the current schema, return its path."""
    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "pq.db"
    conn = get_conn(db_path)
    try:
        conn.executescript(SCHEMA_SQL)
        cur = conn.execute("SELECT version FROM schema_version")
        row = cur.fetchone()
        if row is None:
            conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))
        else:
            conn.execute("UPDATE schema_version SET version = ?", (SCHEMA_VERSION,))
        conn.commit()
    finally:
        conn.close()
    return db_path


def get_conn(db_path: Path) -> sqlite3.Connection:
    """Open a connection with row factory and FK enabled."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn
