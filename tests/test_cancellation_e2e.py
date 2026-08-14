"""End-to-end tests for run cancellation: subprocess kill (Bug 1) and interruptible
backoff (Bug 2)."""
from __future__ import annotations

import json
import textwrap
import threading
import time
from pathlib import Path

from click.testing import CliRunner

from pq import db as db_mod
from pq.cancel import cancel_run
from pq.cli import main
from pq.config import Config
from pq.db import get_conn
from pq.pipelines import Pipeline, Step
from pq.runner import run_step_with_retries
from pq.signals import WorkerStop
from pq.worker import worker_loop


def _make_long_pipeline(tmp_path: Path) -> Path:
    """A pipeline whose single step sleeps 60s. Used to keep a subprocess alive
    long enough for cancel to fire."""
    pipe = tmp_path / "pipe"
    pipe.mkdir()
    (pipe / "prompts").mkdir()
    (pipe / "outputs").mkdir()
    (pipe / "pipeline.yaml").write_text(
        textwrap.dedent("""
            name: longrun
            steps:
              - id: s
                command: sh
                args: ["-c", "sleep 60"]
        """).strip()
    )
    return pipe


def test_cancel_kills_subprocess_in_worker_loop(tmp_path: Path):
    """Bug 1: cancelling a run while the worker is mid-step must SIGKILL the
    active subprocess (which `pq cancel` finds by reading pid from meta.json).
    """
    data_dir = tmp_path / "data"
    pipe = _make_long_pipeline(tmp_path)
    r = CliRunner()
    add_result = r.invoke(main, ["--data-dir", str(data_dir), "add", str(pipe)])
    assert add_result.exit_code == 0, add_result.output

    run_dir = data_dir / "runs" / "1"
    assert (run_dir / "meta.json").exists()

    stop = WorkerStop()
    t = threading.Thread(target=worker_loop, args=(Config(data_dir=data_dir, poll_interval_seconds=0), stop), daemon=True)
    t.start()

    # Poll for meta.json to contain a pid (the subprocess is now running).
    pid_seen = None
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            meta = json.loads((run_dir / "meta.json").read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            time.sleep(0.05)
            continue
        pid_seen = meta.get("pid")
        if pid_seen:
            break
        time.sleep(0.05)
    assert pid_seen, "subprocess pid was never written to meta.json"
    pid = int(pid_seen)

    # Now cancel via the production code path.
    conn = get_conn(data_dir / "pq.db")
    try:
        cancel_run(conn, 1, data_dir=data_dir)
    finally:
        conn.close()

    # The OS-level process must be dead (or at least have exited).
    # Use /proc on Linux to confirm the PID is gone. (os.kill(pid, 0) is
    # cheaper and platform-portable.)
    import errno
    import os

    try:
        os.kill(pid, 0)
        # Still alive; give it a moment then try again.
        deadline2 = time.monotonic() + 5
        alive = True
        while time.monotonic() < deadline2:
            try:
                os.kill(pid, 0)
                time.sleep(0.05)
            except ProcessLookupError:
                alive = False
                break
        assert not alive, f"pid {pid} still alive after cancel"
    except ProcessLookupError:
        pass  # Already dead, which is what we want.

    # Tell the worker to stop and join.
    stop.should_stop = True
    t.join(timeout=5)


def test_backoff_is_interruptible_by_stop(tmp_path: Path):
    """Bug 2: setting stop.should_stop during the backoff window must cause
    run_step_with_retries to return promptly with a failed StepResult, without
    continuing to retry.
    """
    pipe = tmp_path / "pipe"
    pipe.mkdir()
    p = Pipeline(
        name="p",
        dir=pipe,
        cooldown_seconds=0,
        inputs={},
        steps=[],
    )
    # A step that always fails; this guarantees we enter the backoff sleep.
    step = Step(id="a", command="false")

    stop = WorkerStop()
    data_dir = tmp_path / "data"

    def set_stop_after_delay():
        # 30s backoff; signal stop well before that.
        time.sleep(0.5)
        stop.should_stop = True

    setter = threading.Thread(target=set_stop_after_delay, daemon=True)
    setter.start()

    start = time.monotonic()
    result = run_step_with_retries(
        step=step,
        pipeline=p,
        run_id=1,
        data_dir=data_dir,
        run_inputs={},
        max_attempts=5,
        backoff=(30, 30, 30, 30, 30),
        stop=stop,
    )
    elapsed = time.monotonic() - start
    setter.join(timeout=2)

    # Must return promptly (well under 30s; allow generous slack for CI).
    assert elapsed < 5.0, f"backoff was not interruptible: took {elapsed:.2f}s"
    assert result.status == "failed"
    # 1 attempt completed (the one that failed), then early-exit; max_attempts=5
    # would have taken >= (1 + 4 retries) = at least 4 backoffs of 30s = 120s.
    # So we definitely did NOT retry through all 5 attempts.
