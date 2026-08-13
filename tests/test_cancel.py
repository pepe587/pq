from pathlib import Path
import datetime as dt
from pq.db import init_db, get_conn
from pq.cancel import cancel_run


def test_cancel_marks_run(tmp_path: Path):
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    cur = conn.execute(
        "INSERT INTO runs (pipeline_name, pipeline_dir, inputs_json, status, created_at) "
        "VALUES ('a', '', '{}', 'running', ?)",
        (dt.datetime.now().isoformat(),),
    )
    rid = cur.lastrowid
    cancel_run(conn, int(rid))
    cur = conn.execute("SELECT status FROM runs WHERE id=?", (rid,))
    assert cur.fetchone()["status"] == "cancelled"


def test_cancel_kills_active_subprocess(tmp_path: Path):
    import subprocess, time, os
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    proc = subprocess.Popen(["sleep", "60"])
    cur = conn.execute(
        "INSERT INTO runs (pipeline_name, pipeline_dir, inputs_json, status, created_at, started_at) "
        "VALUES ('a', '', '{}', 'running', ?, ?)",
        (dt.datetime.now().isoformat(), dt.datetime.now().isoformat()),
    )
    rid = int(cur.lastrowid)
    # Store pid somewhere the cancel function can find it
    import json
    meta_dir = tmp_path / "runs" / str(rid)
    meta_dir.mkdir(parents=True)
    (meta_dir / "meta.json").write_text(json.dumps({"pid": proc.pid}))
    cancel_run(conn, rid, data_dir=tmp_path)
    proc.wait(timeout=5)
    assert proc.returncode != 0  # killed
