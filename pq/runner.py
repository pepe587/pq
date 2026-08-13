"""Step subprocess execution: env vars, args resolution, skip-if-exists, retries."""
from __future__ import annotations

import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

from pq.pipelines import Pipeline, Step
from pq.iterations import Iteration, expand_iterations


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
            if not _iteration_outputs_exist(it, pipeline_dir):
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


def _run_single_iteration(
    step: Step,
    pipeline: Pipeline,
    run_id: int,
    data_dir: Path,
    run_inputs: dict[str, str],
    iteration: Iteration,
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

    files = iteration.matched_glob if iteration.matched_glob else [None]
    for src in files:
        # For count_from fan-out, derive {i} from the matched file's stem.
        per_file_index = src.stem.split("_")[-1] if src is not None else str(iteration.index)
        args = [a.replace("{i}", per_file_index) for a in step.args]

        with log_path.open("a") as logf:
            proc = subprocess.run(
                [step.command, *args],
                cwd=str(pipeline.dir),
                env=env,
                stdout=logf,
                stderr=subprocess.STDOUT,
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
    attempt: int,
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

    with log_path.open("w") as logf:
        proc = subprocess.run(
            [step.command, *args],
            cwd=str(pipeline.dir),
            env=env,
            stdout=logf,
            stderr=subprocess.STDOUT,
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
) -> StepResult:
    """Run step, retrying on failure with the given backoff (seconds).

    Iterates fan-out: every iteration returned by expand_iterations is run.
    Retry semantics: the WHOLE step (all iterations) is retried on any failure;
    we do not retry per-iteration. This keeps idempotency simple — if a
    step's iterations are not independent, retrying only the failed one
    could leave shared state inconsistent.
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
                step, pipeline, run_id, data_dir, run_inputs, iteration
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
                time.sleep(delay)
    return StepResult(status=last_status, exit_code=last_exit)
