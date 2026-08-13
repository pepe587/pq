from pathlib import Path
import datetime as dt
import textwrap
from click.testing import CliRunner
from pq.cli import main


def _add_failed_run(tmp_path: Path) -> int:
    pipe = tmp_path / "pipe"
    pipe.mkdir()
    (pipe / "prompts").mkdir()
    (pipe / "outputs").mkdir()
    (pipe / "pipeline.yaml").write_text(textwrap.dedent("""
        name: hello
        steps:
          - id: g
            command: echo
    """).strip())
    data_dir = tmp_path / "data"
    r = CliRunner()
    result = r.invoke(main, ["--data-dir", str(data_dir), "add", str(pipe)])
    assert result.exit_code == 0
    # Mark as failed
    from pq.db import init_db, get_conn
    init_db(data_dir)
    conn = get_conn(data_dir / "pq.db")
    conn.execute("UPDATE runs SET status='failed'")
    conn.execute("UPDATE steps SET status='failed'")
    conn.commit()
    cur = conn.execute("SELECT id FROM runs ORDER BY id DESC LIMIT 1")
    return int(cur.fetchone()["id"])


def test_retry_queued_again(tmp_path: Path):
    rid = _add_failed_run(tmp_path)
    data_dir = tmp_path / "data"
    r = CliRunner()
    result = r.invoke(main, ["--data-dir", str(data_dir), "retry", str(rid)])
    assert result.exit_code == 0
    from pq.db import get_conn
    conn = get_conn(data_dir / "pq.db")
    cur = conn.execute("SELECT status FROM runs WHERE id=?", (rid,))
    assert cur.fetchone()["status"] == "queued"
    cur = conn.execute("SELECT status FROM steps WHERE run_id=?", (rid,))
    assert all(row["status"] == "pending" for row in cur.fetchall())


def test_retry_non_failed_is_noop(tmp_path: Path):
    rid = _add_failed_run(tmp_path)
    data_dir = tmp_path / "data"
    from pq.db import get_conn
    conn = get_conn(data_dir / "pq.db")
    conn.execute("UPDATE runs SET status='done' WHERE id=?", (rid,))
    conn.commit()
    r = CliRunner()
    result = r.invoke(main, ["--data-dir", str(data_dir), "retry", str(rid)])
    assert result.exit_code != 0
