"""Step subprocess execution: env vars, args resolution, skip-if-exists, retries."""
from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

from pq.pipelines import Pipeline, Step
from pq.iterations import expand_iterations


@dataclass
class StepResult:
    status: str  # "done" | "skipped" | "failed"
    exit_code: int | None = None


def _resolve_path(pipeline_dir: Path, template: str) -> Path:
    return pipeline_dir / template


def _all_outputs_exist(iteration, pipeline_dir: Path) -> bool:
    for rel, abs_path in iteration.substituted_outputs.items():
        if not abs_path.exists():
            return False
    return True


def _resolve_dependencies(step: Step, pipeline: Pipeline) -> dict[str, str]:
    """Replace {<step_id>} in args with a list-formatted string of resolved paths.

    For needs-resolved steps, we substitute the placeholder with a list of paths
    that the runner will fan out. For now, we keep a single substitution: the
    runner expands the glob and calls the command once per file.

    Returns a mapping of placeholder -> comma-separated string of paths (relative
    to pipeline_dir). The runner handles per-file fan-out by inspecting this.
    """
    deps: dict[str, str] = {}
    for need_id in step.needs:
        for arg in step.args:
            if f"{{{need_id}}}" in arg:
                # find dependency step
                dep = next((s for s in pipeline.steps if s.id == need_id), None)
                if dep is None:
                    continue
                # resolve outputs from the dep step's produces (after iteration)
                # if dep has count, outputs use {i}; the latest run's outputs are read from disk
                if dep.iterates and dep.iterates.count is not None:
                    paths = []
                    for i in range(1, dep.iterates.count + 1):
                        for prod in dep.produces:
                            rel = prod.replace("{i}", str(i))
                            paths.append(rel)
                elif dep.iterates and dep.iterates.count_from is not None:
                    paths = sorted(str(p.relative_to(pipeline.dir)) for p in pipeline.dir.glob(dep.iterates.count_from))
                else:
                    paths = list(dep.produces)
                deps[f"{{{need_id}}}"] = "\n".join(paths)
    return deps


def _build_env(run_id: int, run_inputs: dict[str, str], data_dir: Path, pipeline_name: str) -> dict[str, str]:
    env = os.environ.copy()
    env["PQ_RUN_ID"] = str(run_id)
    env["PQ_DB_PATH"] = str(data_dir / "db" / f"{pipeline_name}.db")
    for k, v in run_inputs.items():
        env[f"PQ_INPUT_{k.upper()}"] = v
    return env


def _build_args(step: Step, pipeline: Pipeline, iteration_index: int) -> list[str]:
    """Substitute {i} and {<step_id>} (the latter as newline-separated path lists)."""
    deps = _resolve_dependencies(step, pipeline)
    out = []
    for arg in step.args:
        new = arg.replace("{i}", str(iteration_index))
        for placeholder, paths in deps.items():
            new = new.replace(placeholder, paths)
        out.append(new)
    return out


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
            all_exist = _all_outputs_exist(iteration, pipeline.dir)
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
