"""Pipeline YAML parsing and validation."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


class PipelineError(Exception):
    """Raised when a pipeline.yaml is invalid."""


_DURATION_RE = re.compile(r"^(\d+)\s*(s|m|h|d)?$")


def parse_duration(s: str) -> int:
    """Parse '4h', '30m', '120s', '2d' or '30' (seconds) into seconds."""
    s = s.strip()
    m = _DURATION_RE.match(s)
    if not m:
        raise PipelineError(f"invalid duration: {s!r}")
    n = int(m.group(1))
    unit = m.group(2) or "s"
    return n * {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]


@dataclass
class Input:
    type: str
    required: bool


@dataclass
class Iterates:
    count: int | None = None
    count_from: str | None = None
    out_template: str | None = None


@dataclass
class Step:
    id: str
    command: str
    args: list[str] = field(default_factory=list)
    needs: list[str] = field(default_factory=list)
    iterates: Iterates | None = None
    produces: list[str] = field(default_factory=list)
    type: str | None = None


@dataclass
class Pipeline:
    name: str
    dir: Path
    cooldown_seconds: int
    inputs: dict[str, Input]
    steps: list[Step]


def _parse_input(name: str, raw: Any) -> Input:
    if not isinstance(raw, dict):
        raise PipelineError(f"input {name!r}: must be a mapping")
    return Input(
        type=raw.get("type", "string"),
        required=bool(raw.get("required", False)),
    )


def _parse_iterates(raw: Any) -> Iterates | None:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise PipelineError("iterates: must be a mapping")
    has_count = "count" in raw
    has_count_from = "count_from" in raw
    count: int | None = None
    count_from: str | None = None
    if has_count:
        c = raw["count"]
        if not isinstance(c, int) or c <= 0:
            raise PipelineError("iterates.count: must be a positive integer")
        count = c
    if has_count_from:
        cf = raw["count_from"]
        if not isinstance(cf, str):
            raise PipelineError("iterates.count_from: must be a string")
        count_from = cf
    return Iterates(
        count=count,
        count_from=count_from,
        out_template=raw.get("out_template"),
    )


def _parse_step(raw: Any) -> Step:
    if not isinstance(raw, dict):
        raise PipelineError("step: must be a mapping")
    if "id" not in raw or not isinstance(raw["id"], str):
        raise PipelineError("step.id: required string")
    if "command" not in raw or not isinstance(raw["command"], str):
        raise PipelineError(f"step {raw.get('id')!r}: command required")
    needs = raw.get("needs", [])
    if not isinstance(needs, list) or not all(isinstance(x, str) for x in needs):
        raise PipelineError(f"step {raw['id']!r}: needs must be a list of strings")
    args = raw.get("args", [])
    if not isinstance(args, list) or not all(isinstance(x, str) for x in args):
        raise PipelineError(f"step {raw['id']!r}: args must be a list of strings")
    produces = raw.get("produces", [])
    if not isinstance(produces, list) or not all(isinstance(x, str) for x in produces):
        raise PipelineError(f"step {raw['id']!r}: produces must be a list of strings")
    return Step(
        id=raw["id"],
        command=raw["command"],
        args=args,
        needs=needs,
        iterates=_parse_iterates(raw.get("iterates")),
        produces=produces,
        type=raw.get("type"),
    )


def load_pipeline(pipeline_dir: Path) -> Pipeline:
    """Load and parse pipeline.yaml. Does not validate; call validate_pipeline."""
    yaml_path = pipeline_dir / "pipeline.yaml"
    if not yaml_path.exists():
        raise PipelineError(f"no pipeline.yaml in {pipeline_dir}")
    with yaml_path.open() as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise PipelineError("pipeline.yaml must be a mapping")

    name = data.get("name")
    if not isinstance(name, str) or not name:
        raise PipelineError("name: required non-empty string")

    cooldown_str = data.get("cooldown", "0s")
    if not isinstance(cooldown_str, str):
        raise PipelineError("cooldown: must be a duration string")
    cooldown_seconds = parse_duration(cooldown_str)

    raw_inputs = data.get("inputs", {}) or {}
    if not isinstance(raw_inputs, dict):
        raise PipelineError("inputs: must be a mapping")
    inputs = {k: _parse_input(k, v) for k, v in raw_inputs.items()}

    raw_steps = data.get("steps", [])
    if not isinstance(raw_steps, list) or not raw_steps:
        raise PipelineError("steps: must be a non-empty list")
    steps = [_parse_step(s) for s in raw_steps]

    return Pipeline(
        name=name,
        dir=pipeline_dir,
        cooldown_seconds=cooldown_seconds,
        inputs=inputs,
        steps=steps,
    )


def validate_pipeline(p: Pipeline) -> None:
    """Run semantic checks: duplicate step ids, unknown needs, cycles, iterates consistency."""
    # iterates: must have exactly one of count or count_from
    for s in p.steps:
        it = s.iterates
        if it is None:
            continue
        if (it.count is None) == (it.count_from is None):
            raise PipelineError(
                f"step {s.id!r}: iterates must have exactly one of 'count' or 'count_from'"
            )

    ids = [s.id for s in p.steps]
    if len(ids) != len(set(ids)):
        seen: set[str] = set()
        for sid in ids:
            if sid in seen:
                raise PipelineError(f"duplicate step id: {sid!r}")
            seen.add(sid)

    id_set = set(ids)
    for s in p.steps:
        for need in s.needs:
            if need not in id_set:
                raise PipelineError(f"step {s.id!r} needs unknown step {need!r}")

    # Cycle detection via DFS
    by_id = {s.id: s for s in p.steps}
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {sid: WHITE for sid in by_id}

    def visit(sid: str) -> None:
        if color[sid] == GRAY:
            raise PipelineError(f"cycle detected involving step {sid!r}")
        if color[sid] == BLACK:
            return
        color[sid] = GRAY
        for need in by_id[sid].needs:
            visit(need)
        color[sid] = BLACK

    for sid in by_id:
        if color[sid] == WHITE:
            visit(sid)
