"""Step subprocess execution: env vars, args resolution, skip-if-exists, retries."""
from __future__ import annotations

import json
import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from pq.pipelines import Pipeline, Step
from pq.iterations import Iteration, expand_iterations
from pq.signals import WorkerStop


@dataclass
class StepResult:
    status: str  # "done" | "skipped" | "failed"
    exit_code: int | None = None


def _iteration_outputs_exist(iteration: Iteration, pipeline_dir: Path) -> bool:
    """True iff every substituted output for this iteration exists on disk."""
    for abs_path in iteration.substituted_outputs.values():
        if not abs_path.exists():
            return False
    return True


def _count_from_outputs_exist(iteration: Iteration, produces: list[str], pipeline_dir: Path) -> bool:
    """For a count_from iter, requires that for every matched file an output file exists."""
    if not produces:
        return False
    template = produces[0]
    for src in iteration.matched_glob:
        rel = template.replace("{i}", src.stem.split("_")[-1])
        if not (pipeline_dir / rel).exists():
            return False
    return True


def _all_iterations_outputs_exist(iterations: list[Iteration], step: Step, pipeline_dir: Path) -> bool:
    """True iff every iteration's outputs are present on disk (skip-if-exists)."""
    for it in iterations:
        if step.iterates is not None and step.iterates.count_from is not None:
            if not _count_from_outputs_exist(it, step.produces, pipeline_dir):
                return False
        else:
            if it.substituted_outputs:
                if not _iteration_outputs_exist(it, pipeline_dir):
                    return False
            else:
                # Non-iterating step: substituted_outputs is empty, so fall
                # back to checking step.produces directly against disk.
                for p in step.produces:
                    if not (pipeline_dir / p).exists():
                        return False
    return True


def _build_env(run_id: int, run_inputs: dict[str, str], data_dir: Path, pipeline_name: str) -> dict[str, str]:
    env = os.environ.copy()
    env["PQ_RUN_ID"] = str(run_id)
    env["PQ_DB_PATH"] = str(data_dir / "db" / f"{pipeline_name}.db")
    for k, v in run_inputs.items():
        env[f"PQ_INPUT_{k.upper()}"] = v
    return env


def _build_args(step: Step, pipeline: Pipeline, iteration_index: int) -> list[str]:
    """Substitute {i} only. Dependency resolution is the scheduler's job."""
    return [arg.replace("{i}", str(iteration_index)) for arg in step.args]


def _wait_or_stop(stop: Optional[WorkerStop], total: float, poll: float = 1.0) -> bool:
    """Sleep up to `total` seconds, returning early if stop.should_stop becomes True.

    Returns True if interrupted by stop, False if the full `total` elapsed.
    """
    if stop is None or total <= 0:
        if total > 0:
            time.sleep(total)
        return False
    end = time.monotonic() + total
    while time.monotonic() < end:
        if stop.should_stop:
            return True
        remaining = end - time.monotonic()
        time.sleep(min(poll, remaining))
    return False


def _write_pid_to_meta(meta_path: Path, pid: int) -> None:
    """Write/extend meta.json with a `pid` field. Preserves existing keys."""
    try:
        meta = json.loads(meta_path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        meta = {}
    meta["pid"] = pid
    meta_path.write_text(json.dumps(meta, indent=2))


def _clear_pid_from_meta(meta_path: Path) -> None:
    """Remove the `pid` field from meta.json, if present. Preserves other keys."""
    try:
        meta = json.loads(meta_path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return
    if "pid" in meta:
        meta.pop("pid", None)
        meta_path.write_text(json.dumps(meta, indent=2))


def _run_subprocess_with_pid_tracking(
    cmd: list[str],
    cwd: str,
    env: dict[str, str],
    logf,
    meta_path: Path,
) -> subprocess.Popen:
    """Popen a subprocess, persist its PID to meta.json for cancel(), wait, clear pid.

    Returns the Popen after wait() completes so the caller can read returncode.
    """
    proc = subprocess.Popen(
        cmd,
        cwd=cwd,
        env=env,
        stdout=logf,
        stderr=subprocess.STDOUT,
    )
    try:
        _write_pid_to_meta(meta_path, proc.pid)
        proc.wait()
    finally:
        _clear_pid_from_meta(meta_path)
    return proc


def _run_single_iteration(
    step: Step,
    pipeline: Pipeline,
    run_id: int,
    data_dir: Path,
    run_inputs: dict[str, str],
    iteration: Iteration,
    stop: Optional[WorkerStop] = None,
) -> StepResult:
    """Run the step once per file in iteration.matched_glob (or once if empty)."""
    if step.produces and iteration.substituted_outputs and _iteration_outputs_exist(iteration, pipeline.dir):
        return StepResult(status="skipped")
    if step.produces and iteration.matched_glob and _count_from_outputs_exist(iteration, step.produces, pipeline.dir):
        return StepResult(status="skipped")

    env = _build_env(run_id, run_inputs, data_dir, pipeline.name)
    env["PQ_PIPELINE_DIR"] = str(pipeline.dir)
    log_dir = data_dir / "runs" / str(run_id) / "steps" / step.id / str(iteration.index)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "log.txt"
    meta_path = data_dir / "runs" / str(run_id) / "meta.json"

    files = iteration.matched_glob if iteration.matched_glob else [None]
    first = True
    for src in files:
        # Between fan-out iterations: if a stop was requested mid-step,
        # abort the remaining iterations for this step. The first iteration
        # always runs (so a pre-set stop in the worker can still complete
        # the current step before exiting).
        if not first and stop is not None and stop.should_stop:
            return StepResult(status="failed", exit_code=None)
        first = False
        # For count_from fan-out, derive {i} from the matched file's stem.
        per_file_index = src.stem.split("_")[-1] if src is not None else str(iteration.index)
        args = [a.replace("{i}", per_file_index) for a in step.args]

        with log_path.open("a") as logf:
            proc = _run_subprocess_with_pid_tracking(
                [step.command, *args],
                cwd=str(pipeline.dir),
                env=env,
                logf=logf,
                meta_path=meta_path,
            )
        (log_dir / "exit_code").write_text(str(proc.returncode))
        if proc.returncode != 0:
            return StepResult(status="failed", exit_code=proc.returncode)

    return StepResult(status="done", exit_code=0)


def run_step(
    step: Step,
    pipeline: Pipeline,
    run_id: int,
    data_dir: Path,
    run_inputs: dict[str, str],
) -> StepResult:
    """Run a single step (no iterates fan-out: caller handles that)."""
    iterations = expand_iterations(step, pipeline.dir)
    iteration = iterations[0]  # runner does NOT fan out here; scheduler does

    # Skip if all declared outputs exist
    if step.produces:
        if step.iterates is None:
            all_exist = all((pipeline.dir / p).exists() for p in step.produces)
        else:
            all_exist = _iteration_outputs_exist(iteration, pipeline.dir)
        if all_exist:
            return StepResult(status="skipped")

    env = _build_env(run_id, run_inputs, data_dir, pipeline.name)
    env["PQ_PIPELINE_DIR"] = str(pipeline.dir)
    args = _build_args(step, pipeline, iteration.index)

    log_dir = data_dir / "runs" / str(run_id) / "steps" / step.id / str(iteration.index)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "log.txt"
    meta_path = data_dir / "runs" / str(run_id) / "meta.json"

    with log_path.open("w") as logf:
        proc = _run_subprocess_with_pid_tracking(
            [step.command, *args],
            cwd=str(pipeline.dir),
            env=env,
            logf=logf,
            meta_path=meta_path,
        )

    (log_dir / "exit_code").write_text(str(proc.returncode))
    if proc.returncode == 0:
        return StepResult(status="done", exit_code=0)
    return StepResult(status="failed", exit_code=proc.returncode)


def run_step_with_retries(
    step: Step,
    pipeline: Pipeline,
    run_id: int,
    data_dir: Path,
    run_inputs: dict[str, str],
    max_attempts: int,
    backoff: tuple[int, ...],
    stop: Optional[WorkerStop] = None,
) -> StepResult:
    """Run step, retrying on failure with the given backoff (seconds).

    Iterates fan-out: every iteration returned by expand_iterations is run.
    Retry semantics: the WHOLE step (all iterations) is retried on any failure;
    we do not retry per-iteration. This keeps idempotency simple — if a
    step's iterations are not independent, retrying only the failed one
    could leave shared state inconsistent.

    If `stop` is provided, the backoff sleep is interruptible: a set
    `stop.should_stop` causes the function to return a failed StepResult
    promptly without further retries.
    """
    iterations = expand_iterations(step, pipeline.dir)

    # Skip check applied once across all iterations.
    if step.produces and _all_iterations_outputs_exist(iterations, step, pipeline.dir):
        return StepResult(status="skipped")

    last_status = "failed"
    last_exit = None
    for attempt in range(1, max_attempts + 1):
        # Reset per-attempt: re-run every iteration from scratch.
        attempt_failed = False
        for iteration in iterations:
            result = _run_single_iteration(
                step, pipeline, run_id, data_dir, run_inputs, iteration, stop=stop
            )
            if result.status == "failed":
                last_status = "failed"
                last_exit = result.exit_code
                attempt_failed = True
                break
            # "skipped" mid-loop means outputs already exist for that iter;
            # treat the whole step as done.
        if not attempt_failed:
            return StepResult(status="done", exit_code=0)

        if attempt < max_attempts:
            delay = backoff[min(attempt - 1, len(backoff) - 1)]
            if delay > 0:
                interrupted = _wait_or_stop(stop, delay)
                if interrupted:
                    return StepResult(status="failed", exit_code=None)
    return StepResult(status=last_status, exit_code=last_exit)
