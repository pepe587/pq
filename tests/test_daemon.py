import json
import time
from pathlib import Path
import textwrap
import datetime as dt
from click.testing import CliRunner
from pq.cli import main
from pq.config import Config
from pq.signals import WorkerStop
from pq.worker import worker_loop


def test_worker_executes_one_pipeline(tmp_path: Path):
    data_dir = tmp_path / "data"
    pipe = tmp_path / "pipe"
    pipe.mkdir()
    (pipe / "prompts").mkdir()
    (pipe / "outputs").mkdir()
    (pipe / "pipeline.yaml").write_text(textwrap.dedent("""
        name: hello
        steps:
          - id: g
            command: sh
            args: ["-c", "echo hi > outputs/x.txt"]
            produces: ["outputs/x.txt"]
    """).strip())
    cfg = Config(data_dir=data_dir, poll_interval_seconds=0)
    r = CliRunner()
    r.invoke(main, ["--data-dir", str(data_dir), "add", str(pipe)])

    stop = WorkerStop()
    stop.should_stop = True  # stop after one run
    worker_loop(cfg, stop)
    assert (pipe / "outputs" / "x.txt").exists()

    from pq.db import get_conn
    conn = get_conn(data_dir / "pq.db")
    cur = conn.execute("SELECT status FROM runs")
    statuses = [r["status"] for r in cur.fetchall()]
    assert "done" in statuses
