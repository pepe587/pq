"""Garbage-collect old runs: delete rows + their runs/<id>/ directories.

By default, runs older than 30 days are candidates, and `failed` and
`cancelled` runs are always kept (an operator may want them as a record).

In dry-run mode (default) the function returns what WOULD be removed
without touching anything. Pass dry_run=False to actually delete.
"""
from __future__ import annotations

import datetime as dt
import json
import shutil
import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass
class GcCandidate:
    run_id: int
    pipeline_name: str
    status: str
    finished_at: str
    age_days: int


def _age_days(finished_at_iso: str, now: dt.datetime) -> int:
    finished = dt.datetime.fromisoformat(finished_at_iso)
    return (now - finished).days


def collect_garbage(
    conn: sqlite3.Connection,
    data_dir: Path,
    older_than_days: int = 30,
    keep_failed: bool = True,
    keep_cancelled: bool = True,
    now: dt.datetime | None = None,
) -> list[GcCandidate]:
    """Identify runs that would be removed by a gc pass. Does NOT delete yet."""
    now = now or dt.datetime.now()
    cutoff_iso = (now - dt.timedelta(days=older_than_days)).isoformat()
    cur = conn.execute(
        "SELECT id, pipeline_name, status, finished_at FROM runs "
        "WHERE finished_at IS NOT NULL AND finished_at <= ? "
        "ORDER BY id ASC",
        (cutoff_iso,),
    )
    candidates: list[GcCandidate] = []
    for row in cur.fetchall():
        if row["status"] == "failed" and keep_failed:
            continue
        if row["status"] == "cancelled" and keep_cancelled:
            continue
        candidates.append(
            GcCandidate(
                run_id=row["id"],
                pipeline_name=row["pipeline_name"],
                status=row["status"],
                finished_at=row["finished_at"],
                age_days=_age_days(row["finished_at"], now),
            )
        )
    return candidates


def apply_garbage(
    conn: sqlite3.Connection,
    data_dir: Path,
    candidates: list[GcCandidate],
) -> int:
    """Delete the runs in `candidates` from the DB and from runs/<id>/ on disk.

    Returns the number actually removed.
    """
    removed = 0
    for c in candidates:
        run_dir = data_dir / "runs" / str(c.run_id)
        try:
            if run_dir.exists():
                shutil.rmtree(run_dir)
        except OSError:
            # If we can't delete the directory, skip the DB deletion too
            # so the user doesn't end up with an orphaned row.
            continue
        conn.execute("DELETE FROM runs WHERE id=?", (c.run_id,))
        removed += 1
    conn.commit()
    return removed


def run_garbage_collection(
    conn: sqlite3.Connection,
    data_dir: Path,
    older_than_days: int = 30,
    keep_failed: bool = True,
    keep_cancelled: bool = True,
    dry_run: bool = True,
) -> tuple[list[GcCandidate], int]:
    """End-to-end GC: identify candidates, optionally delete them.

    Returns (candidates, removed_count). When dry_run=True, removed is 0
    and candidates reflects what WOULD be deleted.
    """
    candidates = collect_garbage(
        conn,
        data_dir,
        older_than_days=older_than_days,
        keep_failed=keep_failed,
        keep_cancelled=keep_cancelled,
    )
    if dry_run or not candidates:
        return candidates, 0
    return candidates, apply_garbage(conn, data_dir, candidates)
