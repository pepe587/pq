"""Daily upload counter helpers."""
from __future__ import annotations

import sqlite3


def get_uploads_today(conn: sqlite3.Connection, day: str) -> int:
    cur = conn.execute("SELECT uploads_count FROM counters WHERE day=?", (day,))
    row = cur.fetchone()
    return int(row["uploads_count"]) if row else 0


def increment_uploads(conn: sqlite3.Connection, day: str) -> int:
    conn.execute(
        "INSERT INTO counters (day, uploads_count) VALUES (?, 1) "
        "ON CONFLICT(day) DO UPDATE SET uploads_count = uploads_count + 1",
        (day,),
    )
    conn.commit()
    return get_uploads_today(conn, day)
