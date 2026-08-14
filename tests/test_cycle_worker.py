"""End-to-end: worker_loop honors cfg.cycle_pipelines by auto-enqueuing."""
from __future__ import annotations

from pathlib import Path
import textwrap

from pq.config import Config
from pq.signals import WorkerStop
from pq.worker import worker_loop
from pq.db import get_conn


def _toy_pipeline(tmp_path: Path, name: str) -> Path:
    p = tmp_path / name
    p.mkdir()
    (p / "pipeline.yaml").write_text(
        textwrap.dedent(f"""
            name: {name}
            steps:
              - id: s1
                command: sh
                args: ["-c", "echo {name} > outputs/{name}.txt"]
                produces: ["outputs/{name}.txt"]
        """).strip()
    )
    (p / "outputs").mkdir()
    return p


def test_worker_auto_enqueues_from_cycle(tmp_path: Path):
    """With cycle_pipelines set, an empty queue gets auto-populated and executed."""
    p_a = _toy_pipeline(tmp_path, "alpha")
    p_b = _toy_pipeline(tmp_path, "beta")
    data = tmp_path / "data"

    # Seed each pipeline with a previous 'done' run so resolve_pipeline_dir works.
    from pq.db import init_db
    import datetime as dt
    init_db(data)
    conn = get_conn(data / "pq.db")
    now = dt.datetime.now().isoformat()
    conn.execute(
        "INSERT INTO runs (pipeline_name, pipeline_dir, inputs_json, status, created_at, finished_at, cooldown_until) "
        "VALUES (?, ?, '{}', 'done', ?, ?, NULL)",
        ("alpha", str(p_a), now, now),
    )
    conn.execute(
        "INSERT INTO runs (pipeline_name, pipeline_dir, inputs_json, status, created_at, finished_at, cooldown_until) "
        "VALUES (?, ?, '{}', 'done', ?, ?, NULL)",
        ("beta", str(p_b), now, now),
    )
    conn.commit()
    conn.close()

    cfg = Config(
        data_dir=data,
        poll_interval_seconds=0,
        max_uploads_per_day=10,
        cycle_pipelines=("alpha", "beta"),
    )
    stop = WorkerStop()
    stop.should_stop = True  # Process at least one iteration, then exit.
    worker_loop(cfg, stop)

    conn = get_conn(data / "pq.db")
    cur = conn.execute("SELECT pipeline_name, status FROM runs WHERE id > 2 ORDER BY id")
    rows = cur.fetchall()
    # After the previous-queued runs (ids 1, 2), the cycle should have
    # enqueued at least one new run.
    assert rows, "expected at least one auto-enqueued run"
    assert rows[0]["pipeline_name"] in ("alpha", "beta")


def test_empty_cycle_does_not_enqueue(tmp_path: Path):
    """With cycle_pipelines = (), an idle worker should not enqueue anything."""
    data = tmp_path / "data"
    cfg = Config(data_dir=data, poll_interval_seconds=0)
    stop = WorkerStop()
    stop.should_stop = True
    worker_loop(cfg, stop)

    conn = get_conn(data / "pq.db")
    cur = conn.execute("SELECT COUNT(*) AS n FROM runs")
    assert cur.fetchone()["n"] == 0
