import time
from pathlib import Path
import textwrap
from click.testing import CliRunner
from pq.cli import main
from pq.config import Config
from pq.signals import WorkerStop
from pq.worker import worker_loop


def test_full_run_end_to_end(tmp_path: Path):
    pipe = tmp_path / "pipe"
    pipe.mkdir()
    (pipe / "prompts").mkdir()
    (pipe / "outputs").mkdir()
    (pipe / "pipeline.yaml").write_text(textwrap.dedent("""
        name: smoke
        steps:
          - id: g
            command: sh
            args: ["-c", "echo hi > outputs/x.txt"]
            produces: ["outputs/x.txt"]
    """).strip())
    data_dir = tmp_path / "data"
    r = CliRunner()
    r.invoke(main, ["--data-dir", str(data_dir), "add", str(pipe), "--input", "topic=t"])
    cfg = Config(data_dir=data_dir, poll_interval_seconds=0)
    stop = WorkerStop()
    stop.should_stop = True
    worker_loop(cfg, stop)
    assert (pipe / "outputs" / "x.txt").exists()
    from pq.db import get_conn
    conn = get_conn(data_dir / "pq.db")
    cur = conn.execute("SELECT status FROM runs")
    assert all(row["status"] == "done" for row in cur.fetchall())
