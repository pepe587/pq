"""Cycle scheduler: pick the next pipeline in the rotation that is due.

When `pq daemon` cannot pull any queued/waiting run, it asks this module for
the next pipeline from the configured `cycle_pipelines` rotation whose
cooldown has elapsed AND (if it has upload steps) the daily upload quota
still has room. The worker then enqueues a fresh run for that pipeline,
which immediately becomes the next `pick_next_run` candidate (and so is
picked on the next worker loop iteration, naturally FIFO-interleaved with
manual `pq add` runs).

The cycle pointer (which pipeline is "next" in rotation) is in-memory only:
on daemon restart it resets to 0. Cooldown and quota state live in the DB;
this module is purely a dispatch helper.
"""
from __future__ import annotations

import datetime as dt
import json
import sqlite3
from pathlib import Path

from pq import counter as counter_mod
from pq.pipelines import Pipeline, load_pipeline


def _pipeline_name_is_due(
    conn: sqlite3.Connection,
    pipeline_name: str,
    today: str,
    max_uploads_per_day: int,
    now: str,
) -> tuple[bool, str | None]:
    """Return (is_due, reason_if_not).

    A pipeline is due if:
      - it has never been queued before (no rows), OR
      - its last 'done' run's cooldown_until is NULL or <= now.
    If its last run had an upload step, today must have quota left.
    """
    cur = conn.execute(
        "SELECT id, pipeline_dir, cooldown_until, status FROM runs "
        "WHERE pipeline_name=? ORDER BY id DESC LIMIT 1",
        (pipeline_name,),
    )
    row = cur.fetchone()
    if row is None:
        return True, None

    if row["status"] not in ("done", None):  # last run still active → skip this round
        # 'done' won't be the last if the pipeline is running. We treat any
        # non-terminal last status as "not due" so the cycle skips it.
        return False, "previous run still active"

    if row["cooldown_until"] is not None and row["cooldown_until"] > now:
        return False, "cooldown active"

    if counter_mod.get_uploads_today(conn, today) >= max_uploads_per_day:
        # If the last run had an upload step, the quota check is binding.
        # We only block on quota if the pipeline ever had an upload step.
        if _pipeline_has_upload(conn, pipeline_name):
            return False, "daily upload quota full"

    return True, None


def _pipeline_has_upload(conn: sqlite3.Connection, pipeline_name: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM runs r JOIN steps s ON s.run_id=r.id "
        "WHERE r.pipeline_name=? AND s.type='upload' LIMIT 1",
        (pipeline_name,),
    )
    return cur.fetchone() is not None


def next_due_pipeline(
    conn: sqlite3.Connection,
    cycle_pipelines: tuple[str, ...],
    today: str,
    max_uploads_per_day: int,
    now: str,
    start_idx: int,
) -> tuple[int, str] | None:
    """Return (new_index, pipeline_name) of the next due pipeline in rotation.

    Scans cycle_pipelines starting at start_idx, wrapping around. Returns
    None if no pipeline is due this turn (caller should sleep and retry).
    Index advances one slot past the dispatched pipeline.
    """
    if not cycle_pipelines:
        return None
    n = len(cycle_pipelines)
    idx = start_idx % n
    for _ in range(n):
        candidate = cycle_pipelines[idx]
        due, _reason = _pipeline_name_is_due(
            conn, candidate, today, max_uploads_per_day, now
        )
        if due:
            return (idx + 1) % n, candidate
        idx = (idx + 1) % n
    return None


def resolve_pipeline_dir(conn: sqlite3.Connection, pipeline_name: str) -> Path | None:
    """Look up the most recent pipeline_dir recorded for this pipeline_name.

    Used by the cycle scheduler to know which on-disk pipeline.yaml to load
    when auto-enqueuing. Returns None if the pipeline has never been queued
    in this data_dir (caller should skip with an informative message).
    """
    cur = conn.execute(
        "SELECT pipeline_dir FROM runs WHERE pipeline_name=? "
        "ORDER BY id DESC LIMIT 1",
        (pipeline_name,),
    )
    row = cur.fetchone()
    return Path(row["pipeline_dir"]) if row and row["pipeline_dir"] else None


def enqueue_cycle_run(
    conn: sqlite3.Connection,
    data_dir: Path,
    pipeline_name: str,
) -> int | None:
    """Enqueue a fresh run for the named pipeline. Returns the run_id, or
    None if the pipeline_dir can't be resolved or the YAML can't be parsed.
    """
    from pq import queue as queue_mod  # avoid circular at module load
    pipe_dir = resolve_pipeline_dir(conn, pipeline_name)
    if pipe_dir is None:
        return None
    try:
        pipe = load_pipeline(pipe_dir)
    except Exception:
        return None
    return queue_mod.enqueue_run(conn, data_dir, pipe, pipe_dir, {})
