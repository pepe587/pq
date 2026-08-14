"""Tests for pq/gc.py and the pq gc CLI subcommand."""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from click.testing import CliRunner

from pq.cli import main
from pq.db import init_db, get_conn
from pq.gc import collect_garbage, apply_garbage, run_garbage_collection


def _make_run(conn, run_id: int, status: str, finished_at: dt.datetime, name: str = "p"):
    conn.execute(
        "INSERT INTO runs (pipeline_name, pipeline_dir, inputs_json, status, created_at, finished_at) "
        "VALUES (?, '/tmp', '{}', ?, ?, ?)",
        (name, status, finished_at.isoformat(), finished_at.isoformat()),
    )
    rd = conn._conn if False else None  # placeholder
    actual = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
    assert actual == run_id, f"expected id={run_id}, got {actual}"


def _seed_dir(conn, run_id: int, data_dir: Path):
    rd = data_dir / "runs" / str(run_id)
    rd.mkdir(parents=True, exist_ok=True)
    (rd / "meta.json").write_text(json.dumps({"run_id": run_id}))


def test_collect_garbage_finds_old_runs(tmp_path: Path):
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    now = dt.datetime.now()
    _make_run(conn, 1, "done", now - dt.timedelta(days=40))
    _make_run(conn, 2, "done", now - dt.timedelta(days=10))
    _seed_dir(conn, 1, tmp_path)
    _seed_dir(conn, 2, tmp_path)
    conn.commit()
    conn.close()

    conn = get_conn(tmp_path / "pq.db")
    cand = collect_garbage(conn, tmp_path, older_than_days=30)
    assert len(cand) == 1
    assert cand[0].run_id == 1
    assert cand[0].age_days == 40


def test_collect_garbage_keeps_failed_by_default(tmp_path: Path):
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    now = dt.datetime.now()
    _make_run(conn, 1, "failed", now - dt.timedelta(days=100))
    _seed_dir(conn, 1, tmp_path)
    conn.commit()
    conn.close()

    conn = get_conn(tmp_path / "pq.db")
    assert collect_garbage(conn, tmp_path) == []  # default keep_failed=True

    conn = get_conn(tmp_path / "pq.db")
    cand = collect_garbage(conn, tmp_path, keep_failed=False)
    assert len(cand) == 1


def test_apply_garbage_deletes_runs_and_dirs(tmp_path: Path):
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    now = dt.datetime.now()
    _make_run(conn, 1, "done", now - dt.timedelta(days=40))
    _seed_dir(conn, 1, tmp_path)
    conn.commit()
    conn.close()

    conn = get_conn(tmp_path / "pq.db")
    candidates, removed = run_garbage_collection(
        conn, tmp_path, older_than_days=30, dry_run=False
    )
    assert removed == 1
    assert not (tmp_path / "runs" / "1").exists()

    conn = get_conn(tmp_path / "pq.db")
    assert conn.execute("SELECT COUNT(*) AS n FROM runs").fetchone()["n"] == 0


def test_gc_cli_dry_run_by_default(tmp_path: Path):
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    now = dt.datetime.now()
    _make_run(conn, 1, "done", now - dt.timedelta(days=100), name="oldie")
    _seed_dir(conn, 1, tmp_path)
    conn.commit()
    conn.close()

    r = CliRunner()
    res = r.invoke(main, ["--data-dir", str(tmp_path), "gc", "--older-than", "30"])
    assert res.exit_code == 0
    assert "dry run" in res.output
    assert "oldie" in res.output

    # Row should still be there.
    conn = get_conn(tmp_path / "pq.db")
    assert conn.execute("SELECT COUNT(*) AS n FROM runs").fetchone()["n"] == 1


def test_gc_cli_apply_removes(tmp_path: Path):
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    now = dt.datetime.now()
    _make_run(conn, 1, "done", now - dt.timedelta(days=100))
    _seed_dir(conn, 1, tmp_path)
    conn.commit()
    conn.close()

    r = CliRunner()
    res = r.invoke(main, [
        "--data-dir", str(tmp_path), "gc", "--older-than", "30", "--apply",
    ])
    assert res.exit_code == 0
    assert "Removed 1" in res.output

    conn = get_conn(tmp_path / "pq.db")
    assert conn.execute("SELECT COUNT(*) AS n FROM runs").fetchone()["n"] == 0
    assert not (tmp_path / "runs" / "1").exists()
