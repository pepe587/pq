from pathlib import Path
import datetime as dt
from pq.db import init_db, get_conn
from pq.scheduler import pick_next_run, mark_upload_done


def _add_run(conn, name: str, status: str = "queued", cooldown_until: str | None = None) -> int:
    cur = conn.execute(
        "INSERT INTO runs (pipeline_name, pipeline_dir, inputs_json, status, created_at, cooldown_until) "
        "VALUES (?, '', '{}', ?, ?, ?)",
        (name, status, dt.datetime.now().isoformat(), cooldown_until),
    )
    return cur.lastrowid  # type: ignore


def test_pick_returns_oldest_queued(tmp_path: Path):
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    a = _add_run(conn, "a")
    b = _add_run(conn, "b")
    assert pick_next_run(conn, dt.datetime.now().isoformat(), 999, "2026-08-14") == a


def test_pick_respects_cooldown(tmp_path: Path):
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    future = (dt.datetime.now() + dt.timedelta(hours=4)).isoformat()
    _add_run(conn, "a", cooldown_until=future)
    assert pick_next_run(conn, dt.datetime.now().isoformat(), 999, "2026-08-14") is None


def test_pick_skips_when_quota_full(tmp_path: Path):
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    # Fill quota for today
    for _ in range(3):
        conn.execute("INSERT OR REPLACE INTO counters (day, uploads_count) VALUES (?, 1)", ("2026-08-14",))
    # But the run isn't an upload type yet. Add an upload step so quota blocks.
    rid = _add_run(conn, "a")
    conn.execute(
        "INSERT INTO steps (run_id, step_id, needs_json, produces_json, type, status) VALUES (?, 'u', '[]', NULL, 'upload', 'pending')",
        (rid,),
    )
    # Set quota to 0 to force block
    assert pick_next_run(conn, dt.datetime.now().isoformat(), 0, "2026-08-14") is None


def test_mark_upload_done_increments(tmp_path: Path):
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    rid = _add_run(conn, "a")
    conn.execute(
        "INSERT INTO steps (run_id, step_id, needs_json, produces_json, type, status) VALUES (?, 'u', '[]', NULL, 'upload', 'pending')",
        (rid,),
    )
    mark_upload_done(conn, rid, "2026-08-14")
    cur = conn.execute("SELECT uploads_count FROM counters WHERE day=?", ("2026-08-14",))
    assert cur.fetchone()["uploads_count"] == 1
