"""Pick the next run to execute, respecting cooldown and quota."""
from __future__ import annotations

import sqlite3


def _has_upload_step(conn: sqlite3.Connection, run_id: int) -> bool:
    cur = conn.execute("SELECT 1 FROM steps WHERE run_id=? AND type='upload' LIMIT 1", (run_id,))
    return cur.fetchone() is not None


def _uploads_today(conn: sqlite3.Connection, day: str) -> int:
    cur = conn.execute("SELECT uploads_count FROM counters WHERE day=?", (day,))
    row = cur.fetchone()
    return int(row["uploads_count"]) if row else 0


def pick_next_run(
    conn: sqlite3.Connection,
    now: str,
    max_uploads_per_day: int,
    today: str,
) -> int | None:
    """Return the id of the next run to execute, or None.

    Selection order:
    1. Status in (queued, waiting), ordered by id ASC (FIFO).
    2. cooldown_until must be NULL or <= now.
    3. If the run contains an upload step, today's counter must be < max.
    """
    cur = conn.execute(
        "SELECT id, cooldown_until FROM runs "
        "WHERE status IN ('queued','waiting') "
        "ORDER BY id ASC"
    )
    for row in cur.fetchall():
        cu = row["cooldown_until"]
        if cu is not None and cu > now:
            continue
        if _has_upload_step(conn, row["id"]) and _uploads_today(conn, today) >= max_uploads_per_day:
            continue
        return int(row["id"])
    return None


def mark_upload_done(conn: sqlite3.Connection, run_id: int, day: str) -> None:
    """Increment today's upload counter and mark the upload step done for this run."""
    conn.execute(
        "UPDATE steps SET status='done' WHERE run_id=? AND type='upload'",
        (run_id,),
    )
    conn.execute(
        "INSERT INTO counters (day, uploads_count) VALUES (?, 1) "
        "ON CONFLICT(day) DO UPDATE SET uploads_count = uploads_count + 1",
        (day,),
    )
    conn.commit()
