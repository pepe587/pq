from pathlib import Path
import sqlite3
from pq.db import init_db, get_conn, SCHEMA_VERSION


def test_init_db_creates_file(tmp_path: Path):
    db_path = init_db(tmp_path)
    assert db_path.exists()
    assert db_path.name == "pq.db"


def test_init_db_idempotent(tmp_path: Path):
    init_db(tmp_path)
    init_db(tmp_path)
    conn = get_conn(init_db(tmp_path))
    cur = conn.execute("SELECT version FROM schema_version")
    assert cur.fetchone()["version"] == SCHEMA_VERSION


def test_schema_has_required_tables(tmp_path: Path):
    db_path = init_db(tmp_path)
    conn = get_conn(db_path)
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = {row["name"] for row in cur.fetchall()}
    assert {"runs", "steps", "step_iterations", "counters", "schema_version"} <= tables
