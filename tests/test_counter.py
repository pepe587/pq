from pathlib import Path
from pq.db import init_db, get_conn
from pq.counter import get_uploads_today, increment_uploads


def test_counter_starts_at_zero(tmp_path: Path):
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    assert get_uploads_today(conn, "2026-08-14") == 0


def test_increment_returns_new_count(tmp_path: Path):
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    assert increment_uploads(conn, "2026-08-14") == 1
    assert increment_uploads(conn, "2026-08-14") == 2
    assert increment_uploads(conn, "2026-08-15") == 1
