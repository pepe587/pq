"""Expand a Step's iterates config into concrete iterations."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from pq.pipelines import Step


@dataclass
class Iteration:
    index: int
    substituted_outputs: dict[str, Path] = field(default_factory=dict)
    matched_glob: list[Path] = field(default_factory=list)


def _substitute(template: str, i: int) -> str:
    return template.replace("{i}", str(i))


def expand_iterations(step: Step, pipeline_dir: Path) -> list[Iteration]:
    """Resolve iterates into a list of Iteration objects.

    For count_from globs, all matched files are grouped into a single iteration
    (the caller's job to invoke the command per-file if needed).
    """
    if step.iterates is None:
        return [Iteration(index=1)]

    if step.iterates.count is not None:
        n = step.iterates.count
        iters = []
        for i in range(1, n + 1):
            subs = {_substitute(p, i): pipeline_dir / _substitute(p, i) for p in step.produces}
            iters.append(Iteration(index=i, substituted_outputs=subs))
        return iters

    assert step.iterates.count_from is not None
    matched = sorted(pipeline_dir.glob(step.iterates.count_from))
    if not matched:
        raise ValueError(f"step {step.id!r}: no files match glob {step.iterates.count_from!r}")
    # We return one iteration that carries the full list; the runner may fan out per-file.
    return [Iteration(index=1, matched_glob=matched)]
