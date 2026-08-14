"""Tests for pq/reap.py — cleaning up orphans left by a crashed daemon."""
from __future__ import annotations

import json
import os
import signal
import subprocess
import time
from pathlib import Path

from pq.db import init_db, get_conn
from pq.reap import reap_stale_runs


def _make_running_row(conn, run_id: int, data_dir: Path, pid: int | None) -> None:
    import datetime as dt
    cur = conn.execute(
        "INSERT INTO runs (pipeline_name, pipeline_dir, inputs_json, status, created_at, started_at) "
        "VALUES ('p', '/tmp', '{}', 'running', ?, ?)",
        (dt.datetime.now().isoformat(), dt.datetime.now().isoformat()),
    )
    actual_id = cur.lastrowid
    assert actual_id == run_id
    conn.execute(
        "INSERT INTO steps (run_id, step_id, status) VALUES (?, 's1', 'running')",
        (run_id,),
    )
    conn.commit()
    if pid is not None:
        run_dir = data_dir / "runs" / str(run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        meta = {"pid": pid}
        (run_dir / "meta.json").write_text(json.dumps(meta))
    conn.close()


def test_reap_kills_orphan_subprocess(tmp_path: Path):
    """A real running subprocess is SIGKILLed; the row becomes 'failed'."""
    init_db(tmp_path)
    data_dir = tmp_path

    # Spawn a long-lived background subprocess we can kill.
    proc = subprocess.Popen(["sleep", "60"])
    try:
        conn = get_conn(data_dir / "pq.db")
        _make_running_row(conn, 1, data_dir, pid=proc.pid)

        # Confirm it's alive before reap.
        os.kill(proc.pid, 0)

        conn = get_conn(data_dir / "pq.db")
        reaped = reap_stale_runs(conn, data_dir)
        assert reaped == 1
        cur = conn.execute("SELECT status, error FROM runs WHERE id=1")
        row = cur.fetchone()
        assert row["status"] == "failed"
        assert "orphan" in (row["error"] or "")
        cur = conn.execute("SELECT status FROM steps WHERE run_id=1")
        assert all(r["status"] == "failed" for r in cur.fetchall())
    finally:
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()

    # Subprocess should be dead.
    with __import__("pytest").raises(ProcessLookupError):
        os.kill(proc.pid, 0)


def test_reap_handles_already_dead_pid(tmp_path: Path):
    """A stale PID whose process is gone → row still marked failed, no crash."""
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    _make_running_row(conn, 1, tmp_path, pid=999_999_999)  # almost certainly unused

    conn = get_conn(tmp_path / "pq.db")
    reaped = reap_stale_runs(conn, tmp_path)
    assert reaped == 1
    cur = conn.execute("SELECT status FROM runs WHERE id=1")
    assert cur.fetchone()["status"] == "failed"


def test_reap_idempotent_on_clean_db(tmp_path: Path):
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    reaped = reap_stale_runs(conn, tmp_path)
    assert reaped == 0


def test_reap_skips_non_running_rows(tmp_path: Path):
    """Done/failed/cancelled rows must NOT be reaped even if meta.json has a pid."""
    import datetime as dt
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    # Insert one done row (should be left alone).
    conn.execute(
        "INSERT INTO runs (pipeline_name, pipeline_dir, inputs_json, status, created_at, finished_at) "
        "VALUES ('p', '/tmp', '{}', 'done', ?, ?)",
        (dt.datetime.now().isoformat(), dt.datetime.now().isoformat()),
    )
    conn.commit()
    conn.close()

    conn = get_conn(tmp_path / "pq.db")
    reaped = reap_stale_runs(conn, tmp_path)
    assert reaped == 0
    cur = conn.execute("SELECT status FROM runs WHERE id=1")
    assert cur.fetchone()["status"] == "done"
