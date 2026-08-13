"""The main worker loop: pick next run, execute, repeat."""
from __future__ import annotations

import datetime as dt
import json
import time
import zoneinfo
from pathlib import Path

from pq import counter as counter_mod
from pq import db as db_mod
from pq import runner as runner_mod
from pq import scheduler as scheduler_mod
from pq.config import Config
from pq.pipelines import load_pipeline, validate_pipeline
from pq.signals import WorkerStop


def _today_in_tz(tz: str) -> str:
    try:
        zone = zoneinfo.ZoneInfo(tz)
    except Exception:
        zone = zoneinfo.ZoneInfo("UTC")
    return dt.datetime.now(zone).date().isoformat()


def _execute_run(conn, run_id: int, data_dir: Path, cfg: Config, today: str) -> None:
    """Run a single run end-to-end."""
    cur = conn.execute("SELECT * FROM runs WHERE id=?", (run_id,))
    run = cur.fetchone()
    if run is None:
        return

    # Re-load pipeline from snapshot
    snapshot_path = data_dir / "runs" / str(run_id) / "meta.json"
    meta = json.loads(snapshot_path.read_text())
    pipeline_dir = Path(meta["pipeline_dir"])
    snapshot = meta.get("snapshot", meta)

    # Topological order
    order = _topo_order_from_snapshot(snapshot, pipeline_dir)

    conn.execute("UPDATE runs SET status='running', started_at=? WHERE id=?",
                 (dt.datetime.now().isoformat(), run_id))
    conn.commit()

    inputs = json.loads(run["inputs_json"]) if run["inputs_json"] else {}

    # Build a synthetic Pipeline from the snapshot dict
    pipe = _pipeline_from_snapshot(snapshot, pipeline_dir)

    cooldown_seconds = 0
    try:
        cd_raw = snapshot.get("cooldown", "0s")
        from pq.pipelines import parse_duration
        cooldown_seconds = parse_duration(cd_raw) if isinstance(cd_raw, str) else int(cd_raw or 0)
    except Exception:
        cooldown_seconds = 0

    failed = False
    for step in order:
        # Re-check quota if this is an upload step
        if step.type == "upload":
            if counter_mod.get_uploads_today(conn, today) >= cfg.max_uploads_per_day:
                conn.execute("UPDATE runs SET status='waiting' WHERE id=?", (run_id,))
                conn.commit()
                return

        conn.execute("UPDATE steps SET status='running' WHERE run_id=? AND step_id=?",
                     (run_id, step.id))
        conn.commit()

        result = runner_mod.run_step_with_retries(
            step=step,
            pipeline=pipe,
            run_id=run_id,
            data_dir=data_dir,
            run_inputs=inputs,
            max_attempts=cfg.max_attempts,
            backoff=cfg.backoff,
        )

        if result.status == "done":
            conn.execute("UPDATE steps SET status='done' WHERE run_id=? AND step_id=?",
                         (run_id, step.id))
            if step.type == "upload":
                scheduler_mod.mark_upload_done(conn, run_id, today)
        elif result.status == "skipped":
            conn.execute("UPDATE steps SET status='done' WHERE run_id=? AND step_id=?",
                         (run_id, step.id))
        else:
            conn.execute("UPDATE steps SET status='failed' WHERE run_id=? AND step_id=?",
                         (run_id, step.id))
            failed = True
            break
        conn.commit()

    if failed:
        conn.execute("UPDATE runs SET status='failed', finished_at=?, error=? WHERE id=?",
                     (dt.datetime.now().isoformat(), "step failed", run_id))
    else:
        finished = dt.datetime.now().isoformat()
        conn.execute("UPDATE runs SET status='done', finished_at=? WHERE id=?", (finished, run_id))
        # Apply cooldown
        cooldown_until = (dt.datetime.fromisoformat(finished) + dt.timedelta(seconds=cooldown_seconds)).isoformat()
        conn.execute("UPDATE runs SET cooldown_until=? WHERE id=?", (cooldown_until, run_id))
    conn.commit()


def _pipeline_from_snapshot(snapshot: dict, pipeline_dir: Path):
    """Rebuild a Pipeline object from a snapshot dict."""
    from pq.pipelines import Pipeline, Step, Iterates, Input
    steps = []
    for s in snapshot.get("steps", []):
        it_raw = s.get("iterates")
        iterates = None
        if it_raw:
            iterates = Iterates(
                count=it_raw.get("count"),
                count_from=it_raw.get("count_from"),
                out_template=it_raw.get("out_template"),
            )
        steps.append(Step(
            id=s["id"],
            command=s["command"],
            args=s.get("args", []),
            needs=s.get("needs", []),
            iterates=iterates,
            produces=s.get("produces", []),
            type=s.get("type"),
        ))
    inputs = {k: Input(type=v.get("type", "string"), required=v.get("required", False))
              for k, v in (snapshot.get("inputs") or {}).items()}

    from pq.pipelines import parse_duration
    cd_raw = snapshot.get("cooldown", "0s")
    cooldown_seconds = parse_duration(cd_raw) if isinstance(cd_raw, str) else int(cd_raw or 0)

    return Pipeline(
        name=snapshot["name"],
        dir=pipeline_dir,
        cooldown_seconds=cooldown_seconds,
        inputs=inputs,
        steps=steps,
    )


def _topo_order_from_snapshot(snapshot: dict, pipeline_dir: Path):
    by_id = {s["id"]: s for s in snapshot.get("steps", [])}
    visited = set()
    order = []
    from pq.pipelines import Step, Iterates

    def visit(sid):
        if sid in visited:
            return
        visited.add(sid)
        for need in by_id[sid].get("needs", []):
            visit(need)
        s = by_id[sid]
        it_raw = s.get("iterates")
        iterates = None
        if it_raw:
            iterates = Iterates(
                count=it_raw.get("count"),
                count_from=it_raw.get("count_from"),
                out_template=it_raw.get("out_template"),
            )
        order.append(Step(
            id=s["id"],
            command=s["command"],
            args=s.get("args", []),
            needs=s.get("needs", []),
            iterates=iterates,
            produces=s.get("produces", []),
            type=s.get("type"),
        ))

    for s in snapshot.get("steps", []):
        visit(s["id"])
    return order


def worker_loop(cfg: Config, stop: WorkerStop) -> None:
    """Main loop. Polls every cfg.poll_interval_seconds when idle.

    Loops while not stop.should_stop. On the FIRST iteration the flag may
    already be set (e.g. a test that wants to run exactly one cycle and then
    exit): we honor that by processing one run before re-checking.
    """
    db_mod.init_db(cfg.data_dir)
    first_iteration = True
    while first_iteration or not stop.should_stop:
        first_iteration = False
        conn = db_mod.get_conn(cfg.data_dir / "pq.db")
        try:
            now = dt.datetime.now().isoformat()
            today = _today_in_tz(cfg.timezone)
            run_id = scheduler_mod.pick_next_run(conn, now, cfg.max_uploads_per_day, today)
            if run_id is None:
                if cfg.poll_interval_seconds > 0:
                    time.sleep(cfg.poll_interval_seconds)
                continue
            _execute_run(conn, run_id, cfg.data_dir, cfg, today)
        finally:
            conn.close()
