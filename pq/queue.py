"""Shared enqueue logic used by `pq add` and the worker's cycle scheduler.

A run is queued by:
  1. computing cooldown_until from the last 'done' run of this pipeline_name,
  2. INSERT into runs with status='queued',
  3. INSERT one row per step into steps,
  4. writing meta.json with the YAML snapshot under data_dir/runs/<run_id>/.

Idempotent callers can reuse the helper instead of duplicating the SQL/text dance.
"""
from __future__ import annotations

import datetime as dt
import json
import sqlite3
from pathlib import Path

import yaml as yaml_lib

from pq import pipeline_db as pipeline_db_mod
from pq.pipelines import Pipeline


def enqueue_run(
    conn: sqlite3.Connection,
    data_dir: Path,
    pipe: Pipeline,
    pipe_path: Path,
    inputs: dict[str, str],
) -> int:
    """Insert a new run row + step rows + meta.json snapshot. Return run id.

    Assumes init_db has already been called on data_dir. Performs the DB
    commit at the end. Caller owns the conn lifetime (this function does
    NOT close it).
    """
    cur = conn.execute(
        "SELECT finished_at FROM runs WHERE pipeline_name=? AND status='done' "
        "ORDER BY finished_at DESC LIMIT 1",
        (pipe.name,),
    )
    row = cur.fetchone()
    cooldown_until = None
    if row and row["finished_at"]:
        finished = dt.datetime.fromisoformat(row["finished_at"])
        cooldown_until = (
            finished + dt.timedelta(seconds=pipe.cooldown_seconds)
        ).isoformat()

    run_id_row = conn.execute(
        "INSERT INTO runs (pipeline_name, pipeline_dir, inputs_json, status, created_at, cooldown_until) "
        "VALUES (?, ?, ?, 'queued', ?, ?)",
        (
            pipe.name,
            str(pipe_path),
            json.dumps(inputs),
            dt.datetime.now().isoformat(),
            cooldown_until,
        ),
    )
    run_id = run_id_row.lastrowid
    if run_id is None:
        raise RuntimeError("failed to insert run")

    for s in pipe.steps:
        conn.execute(
            "INSERT INTO steps (run_id, step_id, needs_json, iterates_json, produces_json, type, status) "
            "VALUES (?, ?, ?, ?, ?, ?, 'pending')",
            (
                run_id,
                s.id,
                json.dumps(s.needs),
                json.dumps({
                    "count": s.iterates.count if s.iterates else None,
                    "count_from": s.iterates.count_from if s.iterates else None,
                    "out_template": s.iterates.out_template if s.iterates else None,
                }),
                json.dumps(s.produces),
                s.type,
            ),
        )

    with (pipe_path / "pipeline.yaml").open() as f:
        yaml_text = f.read()
    snapshot = yaml_lib.safe_load(yaml_text)
    run_dir = data_dir / "runs" / str(run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    meta = {
        "run_id": run_id,
        "pipeline_name": pipe.name,
        "pipeline_dir": str(pipe_path),
        "inputs": inputs,
        "created_at": dt.datetime.now().isoformat(),
        "snapshot": snapshot,
    }
    (run_dir / "meta.json").write_text(json.dumps(meta, indent=2))

    conn.commit()

    pipeline_db_mod.ensure_pipeline_db(data_dir, pipe.name)

    return int(run_id)
