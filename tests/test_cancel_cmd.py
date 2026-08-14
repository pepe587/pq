from pathlib import Path
import datetime as dt
from click.testing import CliRunner
from pq.cli import main


def test_cancel_marks_cancelled(tmp_path: Path):
    from pq.db import init_db, get_conn
    data_dir = tmp_path / "data"
    init_db(data_dir)
    conn = get_conn(data_dir / "pq.db")
    conn.execute(
        "INSERT INTO runs (pipeline_name, pipeline_dir, inputs_json, status, created_at) "
        "VALUES ('a', '', '{}', 'running', ?)",
        (dt.datetime.now().isoformat(),),
    )
    rid = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
    conn.commit()
    conn.close()

    r = CliRunner()
    result = r.invoke(main, ["--data-dir", str(data_dir), "cancel", str(rid)])
    assert result.exit_code == 0
    conn = get_conn(data_dir / "pq.db")
    cur = conn.execute("SELECT status FROM runs WHERE id=?", (rid,))
    assert cur.fetchone()["status"] == "cancelled"


def test_cancel_rejects_done_run(tmp_path: Path):
    """Regression: cancel on a non-active run must NOT silently change its status."""
    from pq.db import init_db, get_conn
    data_dir = tmp_path / "data"
    init_db(data_dir)
    conn = get_conn(data_dir / "pq.db")
    conn.execute(
        "INSERT INTO runs (pipeline_name, pipeline_dir, inputs_json, status, created_at) "
        "VALUES ('a', '', '{}', 'done', ?)",
        (dt.datetime.now().isoformat(),),
    )
    rid = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
    conn.commit()
    conn.close()

    r = CliRunner()
    result = r.invoke(main, ["--data-dir", str(data_dir), "cancel", str(rid)])
    # Must fail loudly (exit 1 + ClickException), not silently overwrite.
    assert result.exit_code == 1
    assert "not active" in (result.output or "")

    # And the DB row must be untouched.
    conn = get_conn(data_dir / "pq.db")
    cur = conn.execute("SELECT status FROM runs WHERE id=?", (rid,))
    assert cur.fetchone()["status"] == "done"
