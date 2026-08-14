"""Tests for the cycle scheduler (pq/cycle.py).

These cover the dispatch logic in isolation: does next_due_pipeline pick the
right pipeline given the DB state, and does enqueue_cycle_run actually
write a queued row with a snapshot? The worker integration is covered by
test_cycle_worker.py.
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

from pq.cycle import next_due_pipeline, enqueue_cycle_run, resolve_pipeline_dir
from pq.db import init_db, get_conn


def _make_run(conn, name: str, status: str = "done", cooldown_until: str | None = None,
              pipeline_dir: str = ""):
    conn.execute(
        "INSERT INTO runs (pipeline_name, pipeline_dir, inputs_json, status, created_at, cooldown_until) "
        "VALUES (?, ?, '{}', ?, ?, ?)",
        (name, pipeline_dir, status, dt.datetime.now().isoformat(), cooldown_until),
    )
    return conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]


def test_first_call_dispatches_first_in_cycle(tmp_path: Path):
    """If a pipeline has never been queued, the cycle should pick it."""
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    picked = next_due_pipeline(
        conn,
        ("a", "b", "c"),
        today="2026-08-14",
        max_uploads_per_day=10,
        now=dt.datetime.now().isoformat(),
        start_idx=0,
    )
    assert picked == (1, "a")


def test_skips_pipeline_with_active_cooldown(tmp_path: Path):
    """A pipeline whose last 'done' run's cooldown_until is in the future must be skipped."""
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    future = (dt.datetime.now() + dt.timedelta(hours=2)).isoformat()
    _make_run(conn, "a", status="done", cooldown_until=future)
    _make_run(conn, "b", status="done", cooldown_until=None)

    picked = next_due_pipeline(
        conn, ("a", "b", "c"), "2026-08-14", 10, dt.datetime.now().isoformat(), 0
    )
    assert picked == (2, "b")  # skipped a, picked b


def test_wraps_around(tmp_path: Path):
    """start_idx past the end wraps to the start."""
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    picked = next_due_pipeline(
        conn, ("a", "b", "c"), "2026-08-14", 10, dt.datetime.now().isoformat(), 2
    )
    assert picked == (0, "c")  # start at idx 2 == c


def test_no_pipeline_due_returns_none(tmp_path: Path):
    """If every pipeline has an active cooldown, returns None."""
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    future = (dt.datetime.now() + dt.timedelta(hours=2)).isoformat()
    _make_run(conn, "a", status="done", cooldown_until=future)
    _make_run(conn, "b", status="done", cooldown_until=future)

    picked = next_due_pipeline(
        conn, ("a", "b"), "2026-08-14", 10, dt.datetime.now().isoformat(), 0
    )
    assert picked is None


def test_empty_cycle_returns_none(tmp_path: Path):
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    picked = next_due_pipeline(
        conn, (), "2026-08-14", 10, dt.datetime.now().isoformat(), 0
    )
    assert picked is None


def test_enqueues_run_for_known_pipeline(tmp_path: Path):
    """enqueue_cycle_run writes a queued run + meta.json with snapshot."""
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")

    # Build a real pipeline dir on disk so the snapshot can be loaded.
    pipe_dir = tmp_path / "cycle-pipe"
    pipe_dir.mkdir()
    (pipe_dir / "pipeline.yaml").write_text(
        "name: cycled\nsteps:\n  - id: s1\n    command: sh\n    args: ['-c', 'echo hi']\n"
    )

    # Seed the DB with a previous run so resolve_pipeline_dir finds the path.
    _make_run(conn, "cycled", status="done", pipeline_dir=str(pipe_dir))

    run_id = enqueue_cycle_run(conn, tmp_path, "cycled")
    assert run_id is not None
    cur = conn.execute("SELECT status, pipeline_name, inputs_json FROM runs WHERE id=?", (run_id,))
    row = cur.fetchone()
    assert row["status"] == "queued"
    assert row["pipeline_name"] == "cycled"

    # meta.json must include the snapshot, like pq add does.
    import json
    meta = json.loads((tmp_path / "runs" / str(run_id) / "meta.json").read_text())
    assert meta["snapshot"]["name"] == "cycled"


def test_resolve_dir_unknown_pipeline_returns_none(tmp_path: Path):
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    assert resolve_pipeline_dir(conn, "never-queued") is None
