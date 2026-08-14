"""Reap orphan runs left behind by a crashed daemon.

When `pq daemon` dies suddenly (SIGKILL, OOM, segfault, power loss), the
subprocess it was executing a step on stays alive but untracked from the
worker's perspective: the row in `runs` keeps status='running' forever.

On every daemon start (before the main loop), `reap_stale_runs` walks all
rows with status='running', reads the PID written to meta.json by the
runner, and SIGKILLs it. The row is then marked 'failed' with error
'orphan from previous daemon' so the operator can decide whether to
`pq retry` it.

Idempotent: re-running on a clean queue is a no-op. A run whose PID has
already exited is handled gracefully (ProcessLookupError) and still gets
marked 'failed'.
"""
from __future__ import annotations

import json
import os
import signal
import sqlite3
from pathlib import Path


def reap_stale_runs(conn: sqlite3.Connection, data_dir: Path) -> int:
    """Kill any orphan subprocess tracked by 'running' rows, then mark them failed.

    Returns the number of runs reaped.
    """
    import datetime as dt
    cur = conn.execute(
        "SELECT id FROM runs WHERE status='running' ORDER BY id ASC"
    )
    run_ids = [row["id"] for row in cur.fetchall()]
    if not run_ids:
        return 0

    now_iso = dt.datetime.now().isoformat()
    reaped = 0
    for run_id in run_ids:
        meta_path = data_dir / "runs" / str(run_id) / "meta.json"
        if not meta_path.exists():
            # No metadata → mark failed without trying to kill anything.
            conn.execute(
                "UPDATE runs SET status='failed', finished_at=?, error=? WHERE id=?",
                (now_iso, "orphan: missing meta.json", run_id),
            )
            conn.execute(
                "UPDATE steps SET status='failed' WHERE run_id=? AND status IN ('pending','running')",
                (run_id,),
            )
            reaped += 1
            continue

        meta = json.loads(meta_path.read_text())
        pid = meta.get("pid")
        if pid is None:
            conn.execute(
                "UPDATE runs SET status='failed', finished_at=?, error=? WHERE id=?",
                (now_iso, "orphan: no pid recorded", run_id),
            )
            conn.execute(
                "UPDATE steps SET status='failed' WHERE run_id=? AND status IN ('pending','running')",
                (run_id,),
            )
            reaped += 1
            continue

        try:
            os.kill(int(pid), signal.SIGKILL)
        except ProcessLookupError:
            pass  # already dead, nothing to do
        except PermissionError:
            # PID belongs to another user (rare); still mark the row failed.
            pass

        conn.execute(
            "UPDATE runs SET status='failed', finished_at=?, error=? WHERE id=?",
            (now_iso, "orphan from previous daemon", run_id),
        )
        conn.execute(
            "UPDATE steps SET status='failed' WHERE run_id=? AND status IN ('pending','running')",
            (run_id,),
        )
        reaped += 1

    conn.commit()
    return reaped
