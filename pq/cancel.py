"""Cancel a run: mark it cancelled, kill any active subprocess."""
from __future__ import annotations

import json
import os
import signal
import sqlite3
from pathlib import Path


def cancel_run(conn: sqlite3.Connection, run_id: int, data_dir: Path | None = None) -> None:
    """Mark the run as cancelled. If a subprocess is running, SIGKILL it."""
    meta_path: Path | None = None
    if data_dir is not None:
        meta_path = data_dir / "runs" / str(run_id) / "meta.json"
    if meta_path is not None and meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text())
            pid = meta.get("pid")
            if pid:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        except (json.JSONDecodeError, KeyError):
            pass
    conn.execute("UPDATE runs SET status='cancelled', finished_at=? WHERE id=?",
                 (__import__("datetime").datetime.now().isoformat(), run_id))
    conn.execute("UPDATE steps SET status='failed' WHERE run_id=? AND status IN ('pending','running')",
                 (run_id,))
    conn.commit()
