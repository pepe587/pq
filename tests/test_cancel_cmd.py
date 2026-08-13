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
