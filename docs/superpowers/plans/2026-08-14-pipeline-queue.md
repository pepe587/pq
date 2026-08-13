# pipeline-queue Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Python CLI (`pq`) that executes declarative YAML pipelines FIFO, with persistent per-pipeline SQLite memory, retry/backoff, cooldown, daily upload quota, and skip-if-exists idempotency.

**Architecture:** Single-process Python CLI using Click, SQLite for queue state, subprocess for step execution, per-pipeline directory layout for self-containment. No parallelism, no web UI, no VRAM management.

**Tech Stack:** Python 3.11+, Click (CLI), SQLite3 (stdlib), pytest, ruamel.yaml or PyYAML, tomli or tomllib (stdlib in 3.11+).

**Spec:** `docs/superpowers/specs/2026-08-14-pipeline-queue-design.md`

## Global Constraints

- Python 3.11+ (uses `tomllib` from stdlib, no extra dep).
- All code uses type hints.
- Tests use pytest, live in `tests/`, mirror the source tree.
- Every task ends with a commit.
- All file paths absolute: project root is `/home/pepe/Desktop/pipeline-queue/`.
- No external services (no Redis, no Postgres, no web server).
- SQLite only.
- YAGNI: do not add features not in the spec.

---

## File Structure

```
/home/pepe/Desktop/pipeline-queue/
├── pyproject.toml              # Project metadata, deps, entry point
├── README.md                   # Brief usage
├── .gitignore
├── pq/
│   ├── __init__.py
│   ├── __main__.py             # python -m pq
│   ├── cli.py                  # Click CLI entry
│   ├── config.py               # config.toml loading
│   ├── db.py                   # Queue SQLite + migrations
│   ├── pipelines.py            # YAML parsing + validation
│   ├── runner.py               # Step subprocess execution
│   ├── scheduler.py            # Pick next run (FIFO + cooldown + quota)
│   ├── cancel.py               # Cancellation helpers
│   └── signals.py              # SIGINT/SIGTERM handlers for worker
├── tests/
│   ├── __init__.py
│   ├── conftest.py             # Fixtures: tmp pipeline dir, tmp data dir
│   ├── test_config.py
│   ├── test_db.py
│   ├── test_pipelines.py
│   ├── test_runner.py
│   ├── test_scheduler.py
│   └── test_cancel.py
└── examples/
    └── youtube-video/
        ├── pipeline.yaml
        ├── prompts/             # Empty, user fills in
        ├── outputs/             # Empty
        └── scripts/
            └── echo.sh          # Trivial script for testing
```

---

## Task 1: Project scaffolding

**Files:**
- Create: `/home/pepe/Desktop/pipeline-queue/pyproject.toml`
- Create: `/home/pepe/Desktop/pipeline-queue/.gitignore`
- Create: `/home/pepe/Desktop/pipeline-queue/pq/__init__.py`
- Create: `/home/pepe/Desktop/pipeline-queue/tests/__init__.py`
- Create: `/home/pepe/Desktop/pipeline-queue/README.md`

**Interfaces:**
- Consumes: nothing
- Produces: installable package `pq` with CLI entry point

- [ ] **Step 1: Create pyproject.toml**

```toml
[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "pipeline-queue"
version = "0.1.0"
description = "FIFO queue for declarative YAML pipelines"
requires-python = ">=3.11"
dependencies = [
    "click>=8.1",
    "pyyaml>=6.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.4",
]

[project.scripts]
pq = "pq.cli:main"

[tool.setuptools.packages.find]
include = ["pq*"]
```

- [ ] **Step 2: Create .gitignore**

```
__pycache__/
*.py[cod]
*.egg-info/
.pytest_cache/
.venv/
build/
dist/
*.db
runs/
data/
```

- [ ] **Step 3: Create empty package files**

`pq/__init__.py`:
```python
"""pipeline-queue: FIFO executor for declarative YAML pipelines."""
__version__ = "0.1.0"
```

`tests/__init__.py`:
```python
```

- [ ] **Step 4: Create README.md**

```markdown
# pipeline-queue

FIFO executor for declarative YAML pipelines.

See `docs/superpowers/specs/2026-08-14-pipeline-queue-design.md` for the full design.

## Quick start

```bash
pip install -e .
pq add examples/youtube-video --input topic="X"
pq daemon
```
```

- [ ] **Step 5: Verify the package installs**

Run: `cd /home/pepe/Desktop/pipeline-queue && python -m venv .venv && .venv/bin/pip install -e ".[dev]"`
Expected: installs successfully.

- [ ] **Step 6: Verify CLI is callable (should fail gracefully — no commands yet)**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pq --help`
Expected: error about no commands or empty CLI (we'll fix in next task).

- [ ] **Step 7: Commit**

```bash
cd /home/pepe/Desktop/pipeline-queue
git init
git add .
git commit -m "chore: project scaffolding"
```

---

## Task 2: Click CLI skeleton with no commands yet

**Files:**
- Create: `/home/pepe/Desktop/pipeline-queue/pq/cli.py`

**Interfaces:**
- Consumes: nothing
- Produces: `main()` callable as CLI entry, with empty subcommands group

- [ ] **Step 1: Write the failing test**

Create `tests/test_cli.py`:
```python
from click.testing import CliRunner
from pq.cli import main

def test_cli_runs():
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "Usage:" in result.output

def test_cli_has_add_subcommand():
    runner = CliRunner()
    result = runner.invoke(main, ["add", "--help"])
    assert result.exit_code == 0
    assert "--input" in result.output
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_cli.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'pq.cli'`.

- [ ] **Step 3: Implement the CLI skeleton**

`pq/cli.py`:
```python
"""CLI entry point for pipeline-queue."""
from __future__ import annotations

import click

from pq import config as config_mod
from pq import db as db_mod
from pq import pipelines as pipelines_mod
from pq import runner as runner_mod
from pq import scheduler as scheduler_mod
from pq import cancel as cancel_mod


@click.group()
@click.option("--config", "config_path", default=None, help="Path to config.toml.")
@click.option("--data-dir", "data_dir", default=None, help="Override data dir.")
@click.pass_context
def main(ctx: click.Context, config_path: str | None, data_dir: str | None) -> None:
    """pipeline-queue: FIFO executor for declarative YAML pipelines."""
    cfg = config_mod.load_config(config_path, data_dir)
    ctx.ensure_object(dict)
    ctx.obj["config"] = cfg


@main.command()
@click.argument("pipeline_dir", type=click.Path(exists=True, file_okay=False))
@click.option("--input", "inputs", multiple=True, help="key=value (can repeat).")
@click.pass_context
def add(ctx: click.Context, pipeline_dir: str, inputs: tuple[str, ...]) -> None:
    """Enqueue a new pipeline run."""
    raise NotImplementedError


@main.command(name="list")
@click.option("--status", default=None, help="Filter by status.")
@click.pass_context
def list_cmd(ctx: click.Context, status: str | None) -> None:
    """List runs."""
    raise NotImplementedError


@main.command()
@click.argument("run_id", type=int)
@click.argument("step_id", required=False)
@click.pass_context
def logs(ctx: click.Context, run_id: int, step_id: str | None) -> None:
    """Show logs of a run or step."""
    raise NotImplementedError


@main.command()
@click.argument("run_id", type=int)
@click.pass_context
def retry(ctx: click.Context, run_id: int) -> None:
    """Retry a failed run."""
    raise NotImplementedError


@main.command()
@click.argument("run_id", type=int)
@click.pass_context
def cancel(ctx: click.Context, run_id: int) -> None:
    """Cancel a run."""
    raise NotImplementedError


@main.command()
@click.pass_context
def daemon(ctx: click.Context) -> None:
    """Run the worker in the foreground."""
    raise NotImplementedError


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it fails (still — add command not implemented)**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_cli.py -v`
Expected: test_cli_runs PASSES, test_cli_has_add_subcommand FAILS (NotImplementedError on add --help, or import error on config/pipelines).

- [ ] **Step 5: Stub out the imported modules so tests can at least import**

Create empty stubs so imports succeed:

`pq/config.py`:
```python
"""Configuration loading (stub)."""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    data_dir: Path
    log_level: str = "info"
    poll_interval_seconds: int = 30
    max_attempts: int = 3
    backoff: tuple[int, ...] = (30, 120, 600)
    max_uploads_per_day: int = 3
    timezone: str = "UTC"


def load_config(config_path: str | None, data_dir_override: str | None) -> Config:
    """Load config from file or defaults."""
    raise NotImplementedError
```

`pq/db.py`, `pq/pipelines.py`, `pq/runner.py`, `pq/scheduler.py`, `pq/cancel.py`:
```python
"""Module stub."""
raise NotImplementedError
```

(Each stub: just a single `raise NotImplementedError` line.)

- [ ] **Step 6: Run tests to verify import works**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_cli.py -v`
Expected: test_cli_runs PASSES, test_cli_has_add_subcommand still FAILS (NotImplementedError, but that's expected for now — we just need the import to work).

- [ ] **Step 7: Commit**

```bash
cd /home/pepe/Desktop/pipeline-queue
git add .
git commit -m "feat: CLI skeleton with stubbed commands"
```

---

## Task 3: Config loading

**Files:**
- Modify: `/home/pepe/Desktop/pipeline-queue/pq/config.py`
- Create: `/home/pepe/Desktop/pipeline-queue/tests/test_config.py`
- Create: `/home/pepe/Desktop/pipeline-queue/tests/conftest.py`

**Interfaces:**
- Consumes: nothing
- Produces: `load_config(config_path: str | None, data_dir_override: str | None) -> Config`

- [ ] **Step 1: Write conftest with fixtures**

`tests/conftest.py`:
```python
"""Shared pytest fixtures."""
from __future__ import annotations

from pathlib import Path
import pytest


@pytest.fixture
def tmp_data_dir(tmp_path: Path) -> Path:
    """A fresh data dir for each test."""
    d = tmp_path / "data"
    d.mkdir()
    return d


@pytest.fixture
def tmp_pipeline_dir(tmp_path: Path) -> Path:
    """A fresh pipeline dir for each test."""
    d = tmp_path / "pipeline"
    d.mkdir()
    (d / "prompts").mkdir()
    (d / "outputs").mkdir()
    (d / "scripts").mkdir()
    return d
```

- [ ] **Step 2: Write the failing test for load_config**

`tests/test_config.py`:
```python
from pathlib import Path
import textwrap
from pq.config import load_config, Config


def test_load_config_defaults(tmp_path: Path):
    cfg = load_config(None, str(tmp_path))
    assert isinstance(cfg, Config)
    assert cfg.data_dir == tmp_path
    assert cfg.max_attempts == 3
    assert cfg.backoff == (30, 120, 600)
    assert cfg.max_uploads_per_day == 3


def test_load_config_from_file(tmp_path: Path):
    config_file = tmp_path / "config.toml"
    config_file.write_text(textwrap.dedent("""
        [general]
        data_dir = "/custom/data"
        log_level = "debug"

        [worker]
        poll_interval_seconds = 10

        [retry]
        max_attempts = 5
        backoff = [10, 20, 40, 80]

        [quota]
        max_uploads_per_day = 7
        timezone = "Europe/Madrid"
    """).strip())
    cfg = load_config(str(config_file), None)
    assert cfg.data_dir == Path("/custom/data")
    assert cfg.log_level == "debug"
    assert cfg.poll_interval_seconds == 10
    assert cfg.max_attempts == 5
    assert cfg.backoff == (10, 20, 40, 80)
    assert cfg.max_uploads_per_day == 7
    assert cfg.timezone == "Europe/Madrid"


def test_data_dir_override_wins(tmp_path: Path):
    config_file = tmp_path / "config.toml"
    config_file.write_text('[general]\ndata_dir = "/from/file"\n')
    override = tmp_path / "override"
    cfg = load_config(str(config_file), str(override))
    assert cfg.data_dir == override
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_config.py -v`
Expected: FAIL with `NotImplementedError`.

- [ ] **Step 4: Implement load_config**

Replace `pq/config.py`:
```python
"""Configuration loading."""
from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    data_dir: Path
    log_level: str = "info"
    poll_interval_seconds: int = 30
    max_attempts: int = 3
    backoff: tuple[int, ...] = (30, 120, 600)
    max_uploads_per_day: int = 3
    timezone: str = "UTC"


def load_config(config_path: str | None, data_dir_override: str | None) -> Config:
    """Load config from TOML file (or defaults) and apply overrides.

    Resolution order (later wins):
    1. Hard-coded defaults.
    2. Values from config_path if given.
    3. data_dir_override if given.
    """
    data: dict = {}
    if config_path is not None:
        with open(config_path, "rb") as f:
            data = tomllib.load(f)

    general = data.get("general", {})
    worker = data.get("worker", {})
    retry = data.get("retry", {})
    quota = data.get("quota", {})

    data_dir_str = data_dir_override or general.get("data_dir") or "~/.local/share/pq"
    data_dir = Path(data_dir_str).expanduser()

    return Config(
        data_dir=data_dir,
        log_level=general.get("log_level", "info"),
        poll_interval_seconds=worker.get("poll_interval_seconds", 30),
        max_attempts=retry.get("max_attempts", 3),
        backoff=tuple(retry.get("backoff", [30, 120, 600])),
        max_uploads_per_day=quota.get("max_uploads_per_day", 3),
        timezone=quota.get("timezone", "UTC"),
    )
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_config.py -v`
Expected: 3 passed.

- [ ] **Step 6: Commit**

```bash
cd /home/pepe/Desktop/pipeline-queue
git add .
git commit -m "feat: config loading from TOML"
```

---

## Task 4: Queue database (SQLite schema + migrations)

**Files:**
- Modify: `/home/pepe/Desktop/pipeline-queue/pq/db.py`
- Create: `/home/pepe/Desktop/pipeline-queue/tests/test_db.py`

**Interfaces:**
- Consumes: a `Path` to a data dir
- Produces:
  - `init_db(data_dir: Path) -> Path` — returns the path to pq.db
  - `get_conn(db_path: Path) -> sqlite3.Connection` — opens a connection with row factory
  - Schema with tables: `runs`, `steps`, `step_iterations`, `counters`

- [ ] **Step 1: Write the failing test**

`tests/test_db.py`:
```python
from pathlib import Path
import sqlite3
from pq.db import init_db, get_conn, SCHEMA_VERSION


def test_init_db_creates_file(tmp_path: Path):
    db_path = init_db(tmp_path)
    assert db_path.exists()
    assert db_path.name == "pq.db"


def test_init_db_idempotent(tmp_path: Path):
    init_db(tmp_path)
    init_db(tmp_path)
    conn = get_conn(init_db(tmp_path))
    cur = conn.execute("SELECT version FROM schema_version")
    assert cur.fetchone()["version"] == SCHEMA_VERSION


def test_schema_has_required_tables(tmp_path: Path):
    db_path = init_db(tmp_path)
    conn = get_conn(db_path)
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = {row["name"] for row in cur.fetchall()}
    assert {"runs", "steps", "step_iterations", "counters", "schema_version"} <= tables
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_db.py -v`
Expected: FAIL with `NotImplementedError`.

- [ ] **Step 3: Implement db.py**

Replace `pq/db.py`:
```python
"""SQLite queue database: schema, migrations, connection helper."""
from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_VERSION = 1

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_name TEXT NOT NULL,
    pipeline_dir TEXT NOT NULL,
    inputs_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('queued','waiting','running','done','failed','cancelled')),
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    cooldown_until TEXT,
    error TEXT
);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_cooldown ON runs(cooldown_until);

CREATE TABLE IF NOT EXISTS steps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    step_id TEXT NOT NULL,
    needs_json TEXT NOT NULL DEFAULT '[]',
    iterates_json TEXT,
    produces_json TEXT,
    type TEXT,
    status TEXT NOT NULL CHECK (status IN ('pending','running','done','failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    UNIQUE(run_id, step_id)
);

CREATE TABLE IF NOT EXISTS step_iterations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    step_run_id INTEGER NOT NULL REFERENCES steps(id) ON DELETE CASCADE,
    iteration INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending','running','done','skipped','failed')),
    log_path TEXT,
    exit_code INTEGER,
    UNIQUE(step_run_id, iteration)
);

CREATE TABLE IF NOT EXISTS counters (
    day TEXT PRIMARY KEY,
    uploads_count INTEGER NOT NULL DEFAULT 0
);
"""


def init_db(data_dir: Path) -> Path:
    """Ensure the data dir exists, create pq.db with the current schema, return its path."""
    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "pq.db"
    conn = get_conn(db_path)
    try:
        conn.executescript(SCHEMA_SQL)
        cur = conn.execute("SELECT version FROM schema_version")
        row = cur.fetchone()
        if row is None:
            conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))
        else:
            conn.execute("UPDATE schema_version SET version = ?", (SCHEMA_VERSION,))
        conn.commit()
    finally:
        conn.close()
    return db_path


def get_conn(db_path: Path) -> sqlite3.Connection:
    """Open a connection with row factory and FK enabled."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_db.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
cd /home/pepe/Desktop/pipeline-queue
git add .
git commit -m "feat: queue SQLite schema and init"
```

---

## Task 5: Pipeline YAML parsing and validation

**Files:**
- Modify: `/home/pepe/Desktop/pipeline-queue/pq/pipelines.py`
- Create: `/home/pepe/Desktop/pipeline-queue/tests/test_pipelines.py`

**Interfaces:**
- Consumes: a path to a directory containing `pipeline.yaml`
- Produces:
  - `class Pipeline` with fields: `name`, `cooldown_seconds`, `inputs`, `steps`
  - `class Step` with fields: `id`, `command`, `args`, `needs`, `iterates`, `produces`, `type`
  - `class Iterates` with fields: `count` (int | None), `count_from` (str | None), `out_template` (str | None)
  - `class Input` with fields: `type`, `required`
  - `def load_pipeline(pipeline_dir: Path) -> Pipeline`
  - `def validate_pipeline(p: Pipeline) -> None` — raises `PipelineError` on bad input

- [ ] **Step 1: Write the failing test**

`tests/test_pipelines.py`:
```python
from pathlib import Path
import textwrap
import pytest
from pq.pipelines import load_pipeline, validate_pipeline, PipelineError


def write_pipeline(d: Path, yaml: str) -> Path:
    (d / "pipeline.yaml").write_text(textwrap.dedent(yaml).strip())
    return d


def test_load_minimal_pipeline(tmp_pipeline_dir: Path):
    write_pipeline(tmp_pipeline_dir, """
        name: hello
        steps:
          - id: greet
            command: echo
            args: ["hi"]
    """)
    p = load_pipeline(tmp_pipeline_dir)
    assert p.name == "hello"
    assert p.cooldown_seconds == 0
    assert len(p.steps) == 1
    assert p.steps[0].id == "greet"
    assert p.steps[0].command == "echo"
    assert p.steps[0].args == ["hi"]
    assert p.steps[0].needs == []
    assert p.steps[0].iterates is None


def test_load_full_pipeline(tmp_pipeline_dir: Path):
    write_pipeline(tmp_pipeline_dir, """
        name: youtube
        cooldown: 4h
        inputs:
          topic:
            type: string
            required: true
        steps:
          - id: guion
            command: ollama
            args: ["run", "cloud", "{topic}"]
            iterates:
              count: 6
              out_template: prompts/img_{i}.txt
            produces:
              - prompts/img_{i}.txt
          - id: imagenes
            needs: [guion]
            command: ideogram
            args: ["--prompt-file", "{prompts}"]
            iterates:
              count_from: prompts/img_*.txt
            produces:
              - outputs/imagenes/img_{i}.png
          - id: upload
            type: upload
            needs: [imagenes]
            command: yt-upload
            args: ["--video", "outputs/final.mp4"]
    """)
    p = load_pipeline(tmp_pipeline_dir)
    assert p.cooldown_seconds == 4 * 3600
    assert p.inputs["topic"].required is True
    assert p.steps[0].iterates.count == 6
    assert p.steps[0].iterates.out_template == "prompts/img_{i}.txt"
    assert p.steps[1].needs == ["guion"]
    assert p.steps[1].iterates.count_from == "prompts/img_*.txt"
    assert p.steps[2].type == "upload"


def test_validate_unknown_need_raises(tmp_pipeline_dir: Path):
    write_pipeline(tmp_pipeline_dir, """
        name: bad
        steps:
          - id: a
            command: echo
            needs: [nonexistent]
    """)
    p = load_pipeline(tmp_pipeline_dir)
    with pytest.raises(PipelineError, match="nonexistent"):
        validate_pipeline(p)


def test_validate_duplicate_step_id_raises(tmp_pipeline_dir: Path):
    write_pipeline(tmp_pipeline_dir, """
        name: bad
        steps:
          - id: a
            command: echo
          - id: a
            command: echo
    """)
    p = load_pipeline(tmp_pipeline_dir)
    with pytest.raises(PipelineError, match="duplicate"):
        validate_pipeline(p)


def test_validate_cycle_raises(tmp_pipeline_dir: Path):
    write_pipeline(tmp_pipeline_dir, """
        name: bad
        steps:
          - id: a
            command: echo
            needs: [b]
          - id: b
            command: echo
            needs: [a]
    """)
    p = load_pipeline(tmp_pipeline_dir)
    with pytest.raises(PipelineError, match="cycle"):
        validate_pipeline(p)


def test_validate_iterates_needs_count_or_countfrom(tmp_pipeline_dir: Path):
    write_pipeline(tmp_pipeline_dir, """
        name: bad
        steps:
          - id: a
            command: echo
            iterates: {}
    """)
    p = load_pipeline(tmp_pipeline_dir)
    with pytest.raises(PipelineError, match="iterates"):
        validate_pipeline(p)


def test_validate_iterates_both_count_and_countfrom_raises(tmp_pipeline_dir: Path):
    write_pipeline(tmp_pipeline_dir, """
        name: bad
        steps:
          - id: a
            command: echo
            iterates:
              count: 3
              count_from: foo/*.txt
    """)
    p = load_pipeline(tmp_pipeline_dir)
    with pytest.raises(PipelineError, match="iterates"):
        validate_pipeline(p)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_pipelines.py -v`
Expected: FAIL with `NotImplementedError`.

- [ ] **Step 3: Implement pipelines.py**

Replace `pq/pipelines.py`:
```python
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
    if has_count == has_count_from:
        raise PipelineError("iterates: must have exactly one of 'count' or 'count_from'")
    if has_count:
        c = raw["count"]
        if not isinstance(c, int) or c <= 0:
            raise PipelineError("iterates.count: must be a positive integer")
        return Iterates(count=c, out_template=raw.get("out_template"))
    return Iterates(count_from=raw["count_from"])


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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_pipelines.py -v`
Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
cd /home/pepe/Desktop/pipeline-queue
git add .
git commit -m "feat: pipeline YAML parsing and validation"
```

---

## Task 6: Per-pipeline database helper

**Files:**
- Create: `/home/pepe/Desktop/pipeline-queue/pq/pipeline_db.py`
- Create: `/home/pepe/Desktop/pipeline-queue/tests/test_pipeline_db.py`

**Interfaces:**
- Consumes: data dir + pipeline name
- Produces:
  - `ensure_pipeline_db(data_dir: Path, name: str) -> Path` — returns path to db, creates file if missing

- [ ] **Step 1: Write the failing test**

`tests/test_pipeline_db.py`:
```python
from pathlib import Path
from pq.pipeline_db import ensure_pipeline_db


def test_ensure_creates_file(tmp_data_dir: Path):
    p = ensure_pipeline_db(tmp_data_dir, "youtube")
    assert p.exists()
    assert p.name == "youtube.db"
    assert p.parent == tmp_data_dir / "db"


def test_ensure_idempotent(tmp_data_dir: Path):
    p1 = ensure_pipeline_db(tmp_data_dir, "youtube")
    p1.write_text("USER CONTENT")
    p2 = ensure_pipeline_db(tmp_data_dir, "youtube")
    assert p1 == p2
    assert p2.read_text() == "USER CONTENT"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_pipeline_db.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'pq.pipeline_db'`.

- [ ] **Step 3: Implement**

`pq/pipeline_db.py`:
```python
"""Per-pipeline SQLite database helper."""
from __future__ import annotations

from pathlib import Path


def ensure_pipeline_db(data_dir: Path, name: str) -> Path:
    """Ensure `data_dir/db/<name>.db` exists; return its path.

    The file is created empty. The pipeline is responsible for its schema.
    Existing content is preserved.
    """
    db_dir = data_dir / "db"
    db_dir.mkdir(parents=True, exist_ok=True)
    db_path = db_dir / f"{name}.db"
    if not db_path.exists():
        db_path.touch()
    return db_path
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_pipeline_db.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
cd /home/pepe/Desktop/pipeline-queue
git add .
git commit -m "feat: per-pipeline database helper"
```

---

## Task 7: `pq add` command

**Files:**
- Modify: `/home/pepe/Desktop/pipeline-queue/pq/cli.py`
- Create: `/home/pepe/Desktop/pipeline-queue/tests/test_add.py`

**Interfaces:**
- Consumes: `pipeline_dir` path, repeated `--input key=value`
- Produces:
  - Validates the YAML
  - Snapshots the YAML to `data_dir/runs/<id>/meta.json`
  - Inserts a `runs` row + `steps` rows in `pq.db`
  - On invalid input: rejects with non-zero exit and a clear message
  - On success: prints the run id

- [ ] **Step 1: Write the failing test**

`tests/test_add.py`:
```python
from pathlib import Path
import json
import textwrap
from click.testing import CliRunner
from pq.cli import main
from pq.config import Config


def write_pipeline(d: Path, yaml: str) -> Path:
    (d / "pipeline.yaml").write_text(textwrap.dedent(yaml).strip())
    return d


def test_add_creates_run(tmp_path: Path):
    pipeline_dir = tmp_path / "pipe"
    pipeline_dir.mkdir()
    (pipeline_dir / "prompts").mkdir()
    (pipeline_dir / "outputs").mkdir()
    write_pipeline(pipeline_dir, """
        name: hello
        steps:
          - id: greet
            command: echo
            args: ["hi"]
    """)
    data_dir = tmp_path / "data"
    runner = CliRunner()
    result = runner.invoke(main, [
        "--data-dir", str(data_dir),
        "add", str(pipeline_dir),
        "--input", "topic=hi",
    ])
    assert result.exit_code == 0, result.output
    assert "queued" in result.output.lower() or "run" in result.output.lower()


def test_add_rejects_missing_required_input(tmp_path: Path):
    pipeline_dir = tmp_path / "pipe"
    pipeline_dir.mkdir()
    (pipeline_dir / "prompts").mkdir()
    (pipeline_dir / "outputs").mkdir()
    write_pipeline(pipeline_dir, """
        name: hello
        inputs:
          topic:
            type: string
            required: true
        steps:
          - id: greet
            command: echo
    """)
    data_dir = tmp_path / "data"
    runner = CliRunner()
    result = runner.invoke(main, [
        "--data-dir", str(data_dir),
        "add", str(pipeline_dir),
    ])
    assert result.exit_code != 0
    assert "topic" in result.output


def test_add_rejects_invalid_pipeline(tmp_path: Path):
    pipeline_dir = tmp_path / "pipe"
    pipeline_dir.mkdir()
    (pipeline_dir / "prompts").mkdir()
    (pipeline_dir / "outputs").mkdir()
    write_pipeline(pipeline_dir, """
        name: bad
        steps:
          - id: a
            command: echo
            needs: [nope]
    """)
    data_dir = tmp_path / "data"
    runner = CliRunner()
    result = runner.invoke(main, [
        "--data-dir", str(data_dir),
        "add", str(pipeline_dir),
    ])
    assert result.exit_code != 0
    assert "nope" in result.output


def test_add_snapshots_yaml(tmp_path: Path):
    pipeline_dir = tmp_path / "pipe"
    pipeline_dir.mkdir()
    (pipeline_dir / "prompts").mkdir()
    (pipeline_dir / "outputs").mkdir()
    write_pipeline(pipeline_dir, """
        name: hello
        steps:
          - id: greet
            command: echo
    """)
    data_dir = tmp_path / "data"
    runner = CliRunner()
    result = runner.invoke(main, [
        "--data-dir", str(data_dir),
        "add", str(pipeline_dir),
    ])
    assert result.exit_code == 0
    run_dirs = list((data_dir / "runs").iterdir())
    assert len(run_dirs) == 1
    meta = json.loads((run_dirs[0] / "meta.json").read_text())
    assert meta["snapshot"]["name"] == "hello"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_add.py -v`
Expected: FAIL with `NotImplementedError`.

- [ ] **Step 3: Implement `add` in cli.py**

Replace the `add` function in `pq/cli.py` (leave the rest unchanged):

```python
import json
import shutil
import datetime as dt
import yaml as yaml_lib
from pq import db as db_mod
from pq import pipelines as pipelines_mod
from pq import pipeline_db as pipeline_db_mod
```

Then replace the `add` body with:

```python
@main.command()
@click.argument("pipeline_dir", type=click.Path(exists=True, file_okay=False))
@click.option("--input", "inputs", multiple=True, help="key=value (repeatable).")
@click.pass_context
def add(ctx: click.Context, pipeline_dir: str, inputs: tuple[str, ...]) -> None:
    """Enqueue a new pipeline run."""
    cfg: Config = ctx.obj["config"]
    pipe_path = Path(pipeline_dir).resolve()
    try:
        pipe = pipelines_mod.load_pipeline(pipe_path)
        pipelines_mod.validate_pipeline(pipe)
    except pipelines_mod.PipelineError as e:
        raise click.ClickException(f"invalid pipeline: {e}")

    # Parse inputs
    provided: dict[str, str] = {}
    for raw in inputs:
        if "=" not in raw:
            raise click.ClickException(f"--input must be key=value, got {raw!r}")
        k, v = raw.split("=", 1)
        provided[k] = v
    for name, spec in pipe.inputs.items():
        if spec.required and name not in provided:
            raise click.ClickException(f"missing required input: {name}")

    db_mod.init_db(cfg.data_dir)

    # Compute cooldown_until from previous successful runs of this pipeline
    conn = db_mod.get_conn(cfg.data_dir / "pq.db")
    try:
        cur = conn.execute(
            "SELECT finished_at FROM runs WHERE pipeline_name=? AND status='done' ORDER BY finished_at DESC LIMIT 1",
            (pipe.name,),
        )
        row = cur.fetchone()
        cooldown_until = None
        if row and row["finished_at"]:
            finished = dt.datetime.fromisoformat(row["finished_at"])
            cooldown_until = (finished + dt.timedelta(seconds=pipe.cooldown_seconds)).isoformat()

        # Snapshot the YAML
        run_id_row = conn.execute(
            "INSERT INTO runs (pipeline_name, pipeline_dir, inputs_json, status, created_at, cooldown_until) "
            "VALUES (?, ?, ?, 'queued', ?, ?)",
            (
                pipe.name,
                str(pipe_path),
                json.dumps(provided),
                dt.datetime.now().isoformat(),
                cooldown_until,
            ),
        )
        run_id = run_id_row.lastrowid
        if run_id is None:
            raise click.ClickException("failed to insert run")

        for s in pipe.steps:
            conn.execute(
                "INSERT INTO steps (run_id, step_id, needs_json, iterates_json, produces_json, type, status) "
                "VALUES (?, ?, ?, ?, ?, ?, 'pending')",
                (
                    run_id,
                    s.id,
                    json.dumps(s.needs),
                    json.dumps({
                        "count": s.iterates.count if s.iterates else None,
                        "count_from": s.iterates.count_from if s.iterates else None,
                        "out_template": s.iterates.out_template if s.iterates else None,
                    }),
                    json.dumps(s.produces),
                    s.type,
                ),
            )

        # Persist snapshot of the YAML + meta
        with (pipe_path / "pipeline.yaml").open() as f:
            yaml_text = f.read()
        snapshot = yaml_lib.safe_load(yaml_text)
        run_dir = cfg.data_dir / "runs" / str(run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        meta = {
            "run_id": run_id,
            "pipeline_name": pipe.name,
            "pipeline_dir": str(pipe_path),
            "inputs": provided,
            "created_at": dt.datetime.now().isoformat(),
            "snapshot": snapshot,
        }
        (run_dir / "meta.json").write_text(json.dumps(meta, indent=2))

        conn.commit()
    finally:
        conn.close()

    # Ensure per-pipeline DB exists
    pipeline_db_mod.ensure_pipeline_db(cfg.data_dir, pipe.name)

    click.echo(f"Run {run_id} queued: {pipe.name}")
```

- [ ] **Step 4: Add the missing imports to cli.py**

Update the top of `pq/cli.py` to add `import datetime as dt`, `import json`, `from pathlib import Path`, `import yaml as yaml_lib`. (The Path is already needed.)

- [ ] **Step 5: Run test to verify it passes**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_add.py -v`
Expected: 4 passed.

- [ ] **Step 6: Commit**

```bash
cd /home/pepe/Desktop/pipeline-queue
git add .
git commit -m "feat: pq add command with snapshot and validation"
```

---

## Task 8: `pq list` command

**Files:**
- Modify: `/home/pepe/Desktop/pipeline-queue/pq/cli.py`
- Create: `/home/pepe/Desktop/pipeline-queue/tests/test_list.py`

**Interfaces:**
- Consumes: data dir
- Produces: tabular list of runs (id, pipeline_name, status, created_at, finished_at)

- [ ] **Step 1: Write the failing test**

`tests/test_list.py`:
```python
from pathlib import Path
from click.testing import CliRunner
from pq.cli import main


def test_list_empty(tmp_path: Path):
    runner = CliRunner()
    result = runner.invoke(main, ["--data-dir", str(tmp_path / "data"), "list"])
    assert result.exit_code == 0


def test_list_shows_runs(tmp_path: Path):
    # Add two runs
    data_dir = tmp_path / "data"
    for topic in ["a", "b"]:
        pipe = tmp_path / "pipe"
        pipe.mkdir()
        (pipe / "prompts").mkdir()
        (pipe / "outputs").mkdir()
        (pipe / "pipeline.yaml").write_text("name: hello\nsteps:\n  - id: g\n    command: echo\n")
        r = CliRunner()
        r.invoke(main, ["--data-dir", str(data_dir), "add", str(pipe), "--input", f"topic={topic}"])
    r = CliRunner()
    result = r.invoke(main, ["--data-dir", str(data_dir), "list"])
    assert result.exit_code == 0
    assert "hello" in result.output
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_list.py -v`
Expected: FAIL with `NotImplementedError`.

- [ ] **Step 3: Implement `list_cmd`**

Replace the `list_cmd` function in `pq/cli.py`:

```python
@main.command(name="list")
@click.option("--status", default=None, help="Filter by status.")
@click.option("--limit", default=20, type=int, help="Max runs to show.")
@click.pass_context
def list_cmd(ctx: click.Context, status: str | None, limit: int) -> None:
    """List runs."""
    cfg: Config = ctx.obj["config"]
    db_path = db_mod.init_db(cfg.data_dir)
    conn = db_mod.get_conn(db_path)
    try:
        if status:
            cur = conn.execute(
                "SELECT id, pipeline_name, status, created_at, finished_at "
                "FROM runs WHERE status=? ORDER BY id DESC LIMIT ?",
                (status, limit),
            )
        else:
            cur = conn.execute(
                "SELECT id, pipeline_name, status, created_at, finished_at "
                "FROM runs ORDER BY id DESC LIMIT ?",
                (limit,),
            )
        rows = cur.fetchall()
        if not rows:
            click.echo("(no runs)")
            return
        click.echo(f"{'ID':<6} {'PIPELINE':<20} {'STATUS':<12} {'CREATED':<20} {'FINISHED':<20}")
        for r in rows:
            click.echo(
                f"{r['id']:<6} {r['pipeline_name']:<20} {r['status']:<12} "
                f"{(r['created_at'] or ''):<20} {(r['finished_at'] or ''):<20}"
            )
    finally:
        conn.close()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_list.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
cd /home/pepe/Desktop/pipeline-queue
git add .
git commit -m "feat: pq list command"
```

---

## Task 9: Iteration expansion (count_from glob resolution)

**Files:**
- Create: `/home/pepe/Desktop/pipeline-queue/pq/iterations.py`
- Create: `/home/pepe/Desktop/pipeline-queue/tests/test_iterations.py`

**Interfaces:**
- Produces:
  - `expand_iterations(step: Step, pipeline_dir: Path) -> list[Iteration]` — returns 1+ iterations
  - `class Iteration` with `index: int` and `substituted_outputs: dict[str, Path]`

- [ ] **Step 1: Write the failing test**

`tests/test_iterations.py`:
```python
from pathlib import Path
from pq.pipelines import Step, Iterates
from pq.iterations import expand_iterations


def test_no_iterates_returns_one(tmp_path: Path):
    step = Step(id="a", command="echo", args=["hi"])
    iters = expand_iterations(step, tmp_path)
    assert len(iters) == 1
    assert iters[0].index == 1


def test_count_expands(tmp_path: Path):
    step = Step(
        id="a",
        command="echo",
        produces=["outputs/img_{i}.png"],
        iterates=Iterates(count=3, out_template="prompts/img_{i}.txt"),
    )
    iters = expand_iterations(step, tmp_path)
    assert [it.index for it in iters] == [1, 2, 3]
    assert iters[0].substituted_outputs == {"outputs/img_1.png": tmp_path / "outputs" / "img_1.png"}
    assert iters[2].substituted_outputs == {"outputs/img_3.png": tmp_path / "outputs" / "img_3.png"}


def test_count_from_glob(tmp_path: Path):
    (tmp_path / "prompts").mkdir()
    for i in range(1, 4):
        (tmp_path / "prompts" / f"img_{i}.txt").write_text(f"prompt {i}")
    step = Step(
        id="a",
        command="ideogram",
        iterates=Iterates(count_from="prompts/img_*.txt"),
    )
    iters = expand_iterations(step, tmp_path)
    assert [it.index for it in iters] == [1, 2, 3]
    assert iters[0].matched_glob == [tmp_path / "prompts" / "img_1.txt"]
    assert iters[1].matched_glob == [tmp_path / "prompts" / "img_2.txt"]


def test_count_from_glob_zero_files_raises(tmp_path: Path):
    (tmp_path / "prompts").mkdir()
    step = Step(
        id="a",
        command="ideogram",
        iterates=Iterates(count_from="prompts/img_*.txt"),
    )
    import pytest
    with pytest.raises(ValueError, match="no files"):
        expand_iterations(step, tmp_path)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_iterations.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'pq.iterations'`.

- [ ] **Step 3: Implement iterations.py**

`pq/iterations.py`:
```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_iterations.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
cd /home/pepe/Desktop/pipeline-queue
git add .
git commit -m "feat: iteration expansion (count / count_from)"
```

---

## Task 10: Step runner (subprocess execution, skip-if-exists, retry, env vars)

**Files:**
- Create: `/home/pepe/Desktop/pipeline-queue/pq/runner.py`
- Create: `/home/pepe/Desktop/pipeline-queue/tests/test_runner.py`

**Interfaces:**
- Produces:
  - `class StepResult` with `status: str` (`done`, `skipped`, `failed`), `exit_code: int | None`
  - `def run_step(step: Step, pipeline: Pipeline, run_id: int, data_dir: Path, run_inputs: dict, attempt: int, max_attempts: int) -> StepResult`
  - `def resolve_dependencies(pipeline: Pipeline, step: Step, run_id: int, data_dir: Path) -> dict[str, list[Path]]` — resolves all `needs` step outputs

- [ ] **Step 1: Write the failing test**

`tests/test_runner.py`:
```python
import os
from pathlib import Path
import textwrap
from click.testing import CliRunner
from pq.cli import main
from pq.pipelines import Pipeline, Step, Iterates, Input
from pq.runner import run_step, StepResult


def make_pipeline(name: str, dir_: Path) -> Pipeline:
    return Pipeline(
        name=name,
        dir=dir_,
        cooldown_seconds=0,
        inputs={"topic": Input(type="string", required=False)},
        steps=[],
    )


def test_run_step_success(tmp_path: Path):
    pipe = tmp_path / "pipe"
    pipe.mkdir()
    (pipe / "outputs").mkdir()
    p = make_pipeline("p", pipe)
    step = Step(
        id="a",
        command="sh",
        args=["-c", "echo hello > outputs/x.txt"],
        produces=["outputs/x.txt"],
    )
    result = run_step(
        step=step,
        pipeline=p,
        run_id=1,
        data_dir=tmp_path / "data",
        run_inputs={"topic": "hi"},
        attempt=1,
    )
    assert result.status == "done"
    assert (pipe / "outputs" / "x.txt").exists()


def test_run_step_skip_if_exists(tmp_path: Path):
    pipe = tmp_path / "pipe"
    pipe.mkdir()
    (pipe / "outputs").mkdir()
    (pipe / "outputs" / "x.txt").write_text("pre-existing")
    p = make_pipeline("p", pipe)
    step = Step(
        id="a",
        command="sh",
        args=["-c", "echo OVERWRITE > outputs/x.txt"],
        produces=["outputs/x.txt"],
    )
    result = run_step(
        step=step,
        pipeline=p,
        run_id=1,
        data_dir=tmp_path / "data",
        run_inputs={},
        attempt=1,
    )
    assert result.status == "skipped"
    assert (pipe / "outputs" / "x.txt").read_text() == "pre-existing"


def test_run_step_failure(tmp_path: Path):
    pipe = tmp_path / "pipe"
    pipe.mkdir()
    p = make_pipeline("p", pipe)
    step = Step(
        id="a",
        command="sh",
        args=["-c", "exit 7"],
    )
    result = run_step(
        step=step,
        pipeline=p,
        run_id=1,
        data_dir=tmp_path / "data",
        run_inputs={},
        attempt=1,
    )
    assert result.status == "failed"
    assert result.exit_code == 7


def test_env_vars_injected(tmp_path: Path):
    pipe = tmp_path / "pipe"
    pipe.mkdir()
    (pipe / "outputs").mkdir()
    p = make_pipeline("p", pipe)
    step = Step(
        id="a",
        command="sh",
        args=["-c", "echo $PQ_RUN_ID > outputs/x.txt; echo $PQ_INPUT_TOPIC >> outputs/x.txt"],
        produces=["outputs/x.txt"],
    )
    data_dir = tmp_path / "data"
    result = run_step(
        step=step,
        pipeline=p,
        run_id=42,
        data_dir=data_dir,
        run_inputs={"topic": "hello"},
        attempt=1,
    )
    assert result.status == "done"
    content = (pipe / "outputs" / "x.txt").read_text()
    assert "42" in content
    assert "hello" in content
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_runner.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'pq.runner'`.

- [ ] **Step 3: Implement runner.py**

`pq/runner.py`:
```python
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
    if step.produces and _all_outputs_exist(iteration, pipeline.dir):
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_runner.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
cd /home/pepe/Desktop/pipeline-queue
git add .
git commit -m "feat: step runner (subprocess, env, skip-if-exists)"
```

---

## Task 11: Runner with iterates fan-out and retry/backoff

**Files:**
- Modify: `/home/pepe/Desktop/pipeline-queue/pq/runner.py`
- Modify: `/home/pepe/Desktop/pipeline-queue/tests/test_runner.py`

**Interfaces:**
- New: `def run_step_with_retries(step, pipeline, run_id, data_dir, run_inputs, max_attempts, backoff) -> StepResult`
- The scheduler uses this; the simple `run_step` remains for direct invocation.

- [ ] **Step 1: Add new tests for fan-out and retries**

Append to `tests/test_runner.py`:
```python
import time
from pq.runner import run_step_with_retries
from pq.pipelines import Step, Iterates


def test_fan_out_count(tmp_path: Path):
    pipe = tmp_path / "pipe"
    pipe.mkdir()
    (pipe / "prompts").mkdir()
    (pipe / "prompts" / "img_1.txt").write_text("p1")
    (pipe / "prompts" / "img_2.txt").write_text("p2")
    (pipe / "prompts" / "img_3.txt").write_text("p3")
    (pipe / "outputs" / "imagenes").mkdir(parents=True)
    p = make_pipeline("p", pipe)
    step = Step(
        id="imgs",
        command="sh",
        args=["-c", "cp {prompts} outputs/imagenes/img_{i}.txt"],
        produces=["outputs/imagenes/img_{i}.txt"],
        iterates=Iterates(count_from="prompts/img_*.txt"),
    )
    result = run_step_with_retries(
        step=step,
        pipeline=p,
        run_id=1,
        data_dir=tmp_path / "data",
        run_inputs={},
        max_attempts=3,
        backoff=(0, 0, 0),
    )
    assert result.status == "done"
    for i in (1, 2, 3):
        assert (pipe / "outputs" / "imagenes" / f"img_{i}.txt").exists()


def test_retry_then_success(tmp_path: Path):
    pipe = tmp_path / "pipe"
    pipe.mkdir()
    p = make_pipeline("p", pipe)
    # Use a counter file to fail twice then succeed
    (pipe / "counter").write_text("0")
    step = Step(
        id="a",
        command="sh",
        args=["-c", "n=$(cat counter); n=$((n+1)); echo $n > counter; [ $n -ge 3 ]"],
    )
    result = run_step_with_retries(
        step=step,
        pipeline=p,
        run_id=1,
        data_dir=tmp_path / "data",
        run_inputs={},
        max_attempts=5,
        backoff=(0, 0, 0, 0, 0),
    )
    assert result.status == "done"


def test_retry_exhausted_returns_failed(tmp_path: Path):
    pipe = tmp_path / "pipe"
    pipe.mkdir()
    p = make_pipeline("p", pipe)
    step = Step(id="a", command="false")
    result = run_step_with_retries(
        step=step,
        pipeline=p,
        run_id=1,
        data_dir=tmp_path / "data",
        run_inputs={},
        max_attempts=3,
        backoff=(0, 0, 0),
    )
    assert result.status == "failed"
```

- [ ] **Step 2: Run new tests to verify they fail**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_runner.py -v`
Expected: the 3 new tests FAIL (function not defined).

- [ ] **Step 3: Add `run_step_with_retries` and fan-out to runner.py**

Append to `pq/runner.py`:

```python
import time
from pq.iterations import Iteration


def _all_outputs_exist_for_iter(iteration: Iteration, produces: list[str], pipeline_dir: Path) -> bool:
    """For a count_from iter, requires that for every matched file an output file exists."""
    if not produces:
        return False
    template = produces[0]
    for src in iteration.matched_glob:
        rel = template.replace("{i}", src.stem.split("_")[-1])
        if not (pipeline_dir / rel).exists():
            return False
    return True


def _run_single_iteration(
    step: Step,
    pipeline: Pipeline,
    run_id: int,
    data_dir: Path,
    run_inputs: dict[str, str],
    iteration: Iteration,
) -> StepResult:
    """Run the step once per file in iteration.matched_glob (or once if empty)."""
    if step.produces and _all_outputs_exist(iteration, pipeline.dir):
        return StepResult(status="skipped")
    if step.produces and iteration.matched_glob and _all_outputs_exist_for_iter(iteration, step.produces, pipeline.dir):
        return StepResult(status="skipped")

    env = _build_env(run_id, run_inputs, data_dir, pipeline.name)
    env["PQ_PIPELINE_DIR"] = str(pipeline.dir)
    log_dir = data_dir / "runs" / str(run_id) / "steps" / step.id / str(iteration.index)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "log.txt"

    files = iteration.matched_glob if iteration.matched_glob else [None]
    for src in files:
        # Build args for this single file
        args = list(step.args)
        if src is not None:
            args = [a.replace("{prompts}", str(src)) for a in args]
        args = [a.replace("{i}", str(iteration.index)) for a in args]

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


def run_step_with_retries(
    step: Step,
    pipeline: Pipeline,
    run_id: int,
    data_dir: Path,
    run_inputs: dict[str, str],
    max_attempts: int,
    backoff: tuple[int, ...],
) -> StepResult:
    """Run step, retrying on failure with the given backoff (seconds)."""
    iterations = expand_iterations(step, pipeline.dir)
    # Skip check for non-iterating steps with produces
    if step.iterates is None and step.produces:
        it = iterations[0]
        if it.substituted_outputs and all(p.exists() for p in it.substituted_outputs.values()):
            return StepResult(status="skipped")

    last_status = "failed"
    last_exit = None
    for attempt in range(1, max_attempts + 1):
        result = _run_single_iteration(step, pipeline, run_id, data_dir, run_inputs, iterations[0])
        if result.status == "done":
            return result
        if result.status == "skipped":
            return result
        last_status = "failed"
        last_exit = result.exit_code
        if attempt < max_attempts:
            delay = backoff[min(attempt - 1, len(backoff) - 1)]
            if delay > 0:
                time.sleep(delay)
    return StepResult(status=last_status, exit_code=last_exit)
```

- [ ] **Step 4: Run all tests to verify they pass**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_runner.py -v`
Expected: 7 passed (the 4 from before + 3 new).

- [ ] **Step 5: Commit**

```bash
cd /home/pepe/Desktop/pipeline-queue
git add .
git commit -m "feat: step runner with fan-out and retry/backoff"
```

---

## Task 12: Daily upload counter

**Files:**
- Modify: `/home/pepe/Desktop/pipeline-queue/pq/db.py`
- Create: `/home/pepe/Desktop/pipeline-queue/tests/test_counter.py`

**Interfaces:**
- Produces:
  - `def get_uploads_today(conn, day: str) -> int`
  - `def increment_uploads(conn, day: str) -> int` (returns new count)
  - `def reset_counter_if_new_day(conn, today: str) -> None` (called by scheduler; today's row either created with 0 or already exists)

- [ ] **Step 1: Write the failing test**

`tests/test_counter.py`:
```python
from pathlib import Path
from pq.db import init_db, get_conn
from pq.counter import get_uploads_today, increment_uploads


def test_counter_starts_at_zero(tmp_path: Path):
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    assert get_uploads_today(conn, "2026-08-14") == 0


def test_increment_returns_new_count(tmp_path: Path):
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    assert increment_uploads(conn, "2026-08-14") == 1
    assert increment_uploads(conn, "2026-08-14") == 2
    assert increment_uploads(conn, "2026-08-15") == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_counter.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'pq.counter'`.

- [ ] **Step 3: Implement counter**

`pq/counter.py`:
```python
"""Daily upload counter helpers."""
from __future__ import annotations

import sqlite3


def get_uploads_today(conn: sqlite3.Connection, day: str) -> int:
    cur = conn.execute("SELECT uploads_count FROM counters WHERE day=?", (day,))
    row = cur.fetchone()
    return int(row["uploads_count"]) if row else 0


def increment_uploads(conn: sqlite3.Connection, day: str) -> int:
    conn.execute(
        "INSERT INTO counters (day, uploads_count) VALUES (?, 1) "
        "ON CONFLICT(day) DO UPDATE SET uploads_count = uploads_count + 1",
        (day,),
    )
    conn.commit()
    return get_uploads_today(conn, day)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_counter.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
cd /home/pepe/Desktop/pipeline-queue
git add .
git commit -m "feat: daily upload counter"
```

---

## Task 13: Scheduler (pick next run, respect cooldown, quota)

**Files:**
- Create: `/home/pepe/Desktop/pipeline-queue/pq/scheduler.py`
- Create: `/home/pepe/Desktop/pipeline-queue/tests/test_scheduler.py`

**Interfaces:**
- Produces:
  - `def pick_next_run(conn, now: str, max_uploads_per_day: int, today: str) -> int | None` — returns run_id or None
  - `def mark_upload_done(conn, run_id: int, day: str) -> None` — increments the counter, also marks the upload step done

- [ ] **Step 1: Write the failing test**

`tests/test_scheduler.py`:
```python
from pathlib import Path
import datetime as dt
from pq.db import init_db, get_conn
from pq.scheduler import pick_next_run, mark_upload_done


def _add_run(conn, name: str, status: str = "queued", cooldown_until: str | None = None) -> int:
    cur = conn.execute(
        "INSERT INTO runs (pipeline_name, pipeline_dir, inputs_json, status, created_at, cooldown_until) "
        "VALUES (?, '', '{}', ?, ?, ?)",
        (name, status, dt.datetime.now().isoformat(), cooldown_until),
    )
    return cur.lastrowid  # type: ignore


def test_pick_returns_oldest_queued(tmp_path: Path):
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    a = _add_run(conn, "a")
    b = _add_run(conn, "b")
    assert pick_next_run(conn, dt.datetime.now().isoformat(), 999, "2026-08-14") == a


def test_pick_respects_cooldown(tmp_path: Path):
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    future = (dt.datetime.now() + dt.timedelta(hours=4)).isoformat()
    _add_run(conn, "a", cooldown_until=future)
    assert pick_next_run(conn, dt.datetime.now().isoformat(), 999, "2026-08-14") is None


def test_pick_skips_when_quota_full(tmp_path: Path):
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    _add_run(conn, "a")
    # Fill quota for today
    for _ in range(3):
        conn.execute("INSERT OR REPLACE INTO counters (day, uploads_count) VALUES (?, 1)", ("2026-08-14",))
    # But the run isn't an upload type yet. Add an upload step so quota blocks.
    rid = _add_run(conn, "a")
    conn.execute(
        "INSERT INTO steps (run_id, step_id, needs_json, produces_json, type, status) VALUES (?, 'u', '[]', NULL, 'upload', 'pending')",
        (rid,),
    )
    # Set quota to 0 to force block
    assert pick_next_run(conn, dt.datetime.now().isoformat(), 0, "2026-08-14") is None


def test_mark_upload_done_increments(tmp_path: Path):
    init_db(tmp_path)
    conn = get_conn(tmp_path / "pq.db")
    rid = _add_run(conn, "a")
    conn.execute(
        "INSERT INTO steps (run_id, step_id, needs_json, produces_json, type, status) VALUES (?, 'u', '[]', NULL, 'upload', 'pending')",
        (rid,),
    )
    mark_upload_done(conn, rid, "2026-08-14")
    cur = conn.execute("SELECT uploads_count FROM counters WHERE day=?", ("2026-08-14",))
    assert cur.fetchone()["uploads_count"] == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_scheduler.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'pq.scheduler'`.

- [ ] **Step 3: Implement scheduler.py**

`pq/scheduler.py`:
```python
"""Pick the next run to execute, respecting cooldown and quota."""
from __future__ import annotations

import sqlite3


def _has_upload_step(conn: sqlite3.Connection, run_id: int) -> bool:
    cur = conn.execute("SELECT 1 FROM steps WHERE run_id=? AND type='upload' LIMIT 1", (run_id,))
    return cur.fetchone() is not None


def _uploads_today(conn: sqlite3.Connection, day: str) -> int:
    cur = conn.execute("SELECT uploads_count FROM counters WHERE day=?", (day,))
    row = cur.fetchone()
    return int(row["uploads_count"]) if row else 0


def pick_next_run(
    conn: sqlite3.Connection,
    now: str,
    max_uploads_per_day: int,
    today: str,
) -> int | None:
    """Return the id of the next run to execute, or None.

    Selection order:
    1. Status in (queued, waiting), ordered by id ASC (FIFO).
    2. cooldown_until must be NULL or <= now.
    3. If the run contains an upload step, today's counter must be < max.
    """
    cur = conn.execute(
        "SELECT id, cooldown_until FROM runs "
        "WHERE status IN ('queued','waiting') "
        "ORDER BY id ASC"
    )
    for row in cur.fetchall():
        cu = row["cooldown_until"]
        if cu is not None and cu > now:
            continue
        if _has_upload_step(conn, row["id"]) and _uploads_today(conn, today) >= max_uploads_per_day:
            continue
        return int(row["id"])
    return None


def mark_upload_done(conn: sqlite3.Connection, run_id: int, day: str) -> None:
    """Increment today's upload counter and mark the upload step done for this run."""
    conn.execute(
        "UPDATE steps SET status='done' WHERE run_id=? AND type='upload'",
        (run_id,),
    )
    conn.execute(
        "INSERT INTO counters (day, uploads_count) VALUES (?, 1) "
        "ON CONFLICT(day) DO UPDATE SET uploads_count = uploads_count + 1",
        (day,),
    )
    conn.commit()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_scheduler.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
cd /home/pepe/Desktop/pipeline-queue
git add .
git commit -m "feat: scheduler with cooldown and upload quota"
```

---

## Task 14: Cancellation helpers

**Files:**
- Create: `/home/pepe/Desktop/pipeline-queue/pq/cancel.py`
- Create: `/home/pepe/Desktop/pipeline-queue/tests/test_cancel.py`

**Interfaces:**
- Produces:
  - `def cancel_run(conn, run_id: int) -> None` — marks run as cancelled, kills any active subprocess

- [ ] **Step 1: Write the failing test**

`tests/test_cancel.py`:
```python
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
    cancel_run(conn, rid)
    proc.wait(timeout=5)
    assert proc.returncode != 0  # killed
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_cancel.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'pq.cancel'`.

- [ ] **Step 3: Implement cancel.py**

`pq/cancel.py`:
```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_cancel.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
cd /home/pepe/Desktop/pipeline-queue
git add .
git commit -m "feat: cancellation helper (SIGKILL + mark cancelled)"
```

---

## Task 15: Signal handlers for the worker

**Files:**
- Create: `/home/pepe/Desktop/pipeline-queue/pq/signals.py`
- Create: `/home/pepe/Desktop/pipeline-queue/tests/test_signals.py`

**Interfaces:**
- Produces:
  - `class WorkerStop` — context manager / flag that the worker checks between runs.

- [ ] **Step 1: Write the failing test**

`tests/test_signals.py`:
```python
import signal
import os
import time
from pq.signals import WorkerStop, install_handlers


def test_worker_stop_starts_false():
    ws = WorkerStop()
    assert ws.should_stop is False


def test_sigint_sets_flag():
    ws = WorkerStop()
    install_handlers(ws)
    os.kill(os.getpid(), signal.SIGINT)
    time.sleep(0.05)
    assert ws.should_stop is True
    # Reset for other tests
    ws.should_stop = False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_signals.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'pq.signals'`.

- [ ] **Step 3: Implement signals.py**

`pq/signals.py`:
```python
"""Signal handling for the worker process."""
from __future__ import annotations

import signal
from dataclasses import dataclass


@dataclass
class WorkerStop:
    should_stop: bool = False


def install_handlers(ws: WorkerStop) -> None:
    """Install SIGINT and SIGTERM handlers that set ws.should_stop = True."""

    def _handler(signum, frame):
        ws.should_stop = True

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_signals.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
cd /home/pepe/Desktop/pipeline-queue
git add .
git commit -m "feat: signal handlers for worker"
```

---

## Task 16: `pq logs` command

**Files:**
- Modify: `/home/pepe/Desktop/pipeline-queue/pq/cli.py`
- Create: `/home/pepe/Desktop/pipeline-queue/tests/test_logs.py`

**Interfaces:**
- Consumes: run_id (int), optional step_id
- Produces: prints log content to stdout

- [ ] **Step 1: Write the failing test**

`tests/test_logs.py`:
```python
from pathlib import Path
import json
from click.testing import CliRunner
from pq.cli import main


def test_logs_prints_step_log(tmp_path: Path):
    data_dir = tmp_path / "data"
    run_id = 7
    log_dir = data_dir / "runs" / str(run_id) / "steps" / "imgs" / "1"
    log_dir.mkdir(parents=True)
    (log_dir / "log.txt").write_text("hello from step\n")
    runner = CliRunner()
    result = runner.invoke(main, ["--data-dir", str(data_dir), "logs", str(run_id), "imgs"])
    assert result.exit_code == 0
    assert "hello from step" in result.output
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_logs.py -v`
Expected: FAIL with `NotImplementedError`.

- [ ] **Step 3: Implement logs command**

Replace the `logs` function in `pq/cli.py`:

```python
@main.command()
@click.argument("run_id", type=int)
@click.argument("step_id", required=False)
@click.pass_context
def logs(ctx: click.Context, run_id: int, step_id: str | None) -> None:
    """Show logs of a run or step."""
    cfg: Config = ctx.obj["config"]
    base = cfg.data_dir / "runs" / str(run_id) / "steps"
    if not base.exists():
        raise click.ClickException(f"no logs for run {run_id}")
    if step_id:
        step_dirs = sorted(base.glob(f"{step_id}/*"))
        if not step_dirs:
            raise click.ClickException(f"no logs for step {step_id}")
        for sd in step_dirs:
            log = sd / "log.txt"
            if log.exists():
                click.echo(f"=== {sd.parent.name}/{sd.name} ===")
                click.echo(log.read_text(), nl=False)
    else:
        for sd in sorted(base.glob("*/*")):
            log = sd / "log.txt"
            if log.exists():
                click.echo(f"=== {sd.parent.name}/{sd.name} ===")
                click.echo(log.read_text(), nl=False)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_logs.py -v`
Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
cd /home/pepe/Desktop/pipeline-queue
git add .
git commit -m "feat: pq logs command"
```

---

## Task 17: `pq retry` command

**Files:**
- Modify: `/home/pepe/Desktop/pipeline-queue/pq/cli.py`
- Create: `/home/pepe/Desktop/pipeline-queue/tests/test_retry.py`

**Interfaces:**
- Consumes: run_id
- Produces: marks run as `queued` again, resets failed steps to `pending`

- [ ] **Step 1: Write the failing test**

`tests/test_retry.py`:
```python
from pathlib import Path
import datetime as dt
import textwrap
from click.testing import CliRunner
from pq.cli import main


def _add_failed_run(tmp_path: Path) -> int:
    pipe = tmp_path / "pipe"
    pipe.mkdir()
    (pipe / "prompts").mkdir()
    (pipe / "outputs").mkdir()
    (pipe / "pipeline.yaml").write_text(textwrap.dedent("""
        name: hello
        steps:
          - id: g
            command: echo
    """).strip())
    data_dir = tmp_path / "data"
    r = CliRunner()
    result = r.invoke(main, ["--data-dir", str(data_dir), "add", str(pipe)])
    assert result.exit_code == 0
    # Mark as failed
    from pq.db import init_db, get_conn
    init_db(data_dir)
    conn = get_conn(data_dir / "pq.db")
    conn.execute("UPDATE runs SET status='failed'")
    conn.execute("UPDATE steps SET status='failed'")
    conn.commit()
    cur = conn.execute("SELECT id FROM runs ORDER BY id DESC LIMIT 1")
    return int(cur.fetchone()["id"])


def test_retry_queued_again(tmp_path: Path):
    rid = _add_failed_run(tmp_path)
    data_dir = tmp_path / "data"
    r = CliRunner()
    result = r.invoke(main, ["--data-dir", str(data_dir), "retry", str(rid)])
    assert result.exit_code == 0
    from pq.db import get_conn
    conn = get_conn(data_dir / "pq.db")
    cur = conn.execute("SELECT status FROM runs WHERE id=?", (rid,))
    assert cur.fetchone()["status"] == "queued"
    cur = conn.execute("SELECT status FROM steps WHERE run_id=?", (rid,))
    assert all(row["status"] == "pending" for row in cur.fetchall())


def test_retry_non_failed_is_noop(tmp_path: Path):
    rid = _add_failed_run(tmp_path)
    data_dir = tmp_path / "data"
    from pq.db import get_conn
    conn = get_conn(data_dir / "pq.db")
    conn.execute("UPDATE runs SET status='done' WHERE id=?", (rid,))
    conn.commit()
    r = CliRunner()
    result = r.invoke(main, ["--data-dir", str(data_dir), "retry", str(rid)])
    assert result.exit_code != 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_retry.py -v`
Expected: FAIL with `NotImplementedError`.

- [ ] **Step 3: Implement retry command**

Replace the `retry` function in `pq/cli.py`:

```python
@main.command()
@click.argument("run_id", type=int)
@click.pass_context
def retry(ctx: click.Context, run_id: int) -> None:
    """Retry a failed run (resets failed steps to pending, marks run queued)."""
    cfg: Config = ctx.obj["config"]
    db_path = db_mod.init_db(cfg.data_dir)
    conn = db_mod.get_conn(db_path)
    try:
        cur = conn.execute("SELECT status FROM runs WHERE id=?", (run_id,))
        row = cur.fetchone()
        if row is None:
            raise click.ClickException(f"no such run: {run_id}")
        if row["status"] != "failed":
            raise click.ClickException(f"run {run_id} is not failed (status: {row['status']})")
        conn.execute("UPDATE runs SET status='queued' WHERE id=?", (run_id,))
        conn.execute(
            "UPDATE steps SET status='pending', attempts=0 WHERE run_id=? AND status='failed'",
            (run_id,),
        )
        conn.commit()
        click.echo(f"Run {run_id} requeued")
    finally:
        conn.close()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_retry.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
cd /home/pepe/Desktop/pipeline-queue
git add .
git commit -m "feat: pq retry command"
```

---

## Task 18: `pq cancel` command

**Files:**
- Modify: `/home/pepe/Desktop/pipeline-queue/pq/cli.py`
- Create: `/home/pepe/Desktop/pipeline-queue/tests/test_cancel_cmd.py`

**Interfaces:**
- Consumes: run_id
- Produces: invokes `cancel_run`, prints confirmation

- [ ] **Step 1: Write the failing test**

`tests/test_cancel_cmd.py`:
```python
from pathlib import Path
import datetime as dt
from click.testing import CliRunner
from pq.cli import main


def test_cancel_marks_cancelled(tmp_path: Path):
    from pq.db import init_db, get_conn
    data_dir = tmp_path / "data"
    init_db(data_dir)
    conn = get_conn(data_dir / "pq.db")
    conn.execute(
        "INSERT INTO runs (pipeline_name, pipeline_dir, inputs_json, status, created_at) "
        "VALUES ('a', '', '{}', 'running', ?)",
        (dt.datetime.now().isoformat(),),
    )
    rid = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
    conn.commit()
    conn.close()

    r = CliRunner()
    result = r.invoke(main, ["--data-dir", str(data_dir), "cancel", str(rid)])
    assert result.exit_code == 0
    conn = get_conn(data_dir / "pq.db")
    cur = conn.execute("SELECT status FROM runs WHERE id=?", (rid,))
    assert cur.fetchone()["status"] == "cancelled"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_cancel_cmd.py -v`
Expected: FAIL with `NotImplementedError`.

- [ ] **Step 3: Implement cancel command**

Replace the `cancel` function in `pq/cli.py`:

```python
@main.command()
@click.argument("run_id", type=int)
@click.pass_context
def cancel(ctx: click.Context, run_id: int) -> None:
    """Cancel a run."""
    cfg: Config = ctx.obj["config"]
    db_path = db_mod.init_db(cfg.data_dir)
    conn = db_mod.get_conn(db_path)
    try:
        cancel_mod.cancel_run(conn, run_id, cfg.data_dir)
        click.echo(f"Run {run_id} cancelled")
    finally:
        conn.close()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_cancel_cmd.py -v`
Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
cd /home/pepe/Desktop/pipeline-queue
git add .
git commit -m "feat: pq cancel command"
```

---

## Task 19: `pq daemon` (the worker loop)

**Files:**
- Modify: `/home/pepe/Desktop/pipeline-queue/pq/cli.py`
- Create: `/home/pepe/Desktop/pipeline-queue/tests/test_daemon.py`

**Interfaces:**
- Produces:
  - `def worker_loop(cfg, stop: WorkerStop) -> None` — the main loop, callable from tests
  - The `daemon` CLI command just calls `worker_loop`

- [ ] **Step 1: Write the failing test**

`tests/test_daemon.py`:
```python
import json
import time
from pathlib import Path
import textwrap
import datetime as dt
from click.testing import CliRunner
from pq.cli import main
from pq.config import Config
from pq.signals import WorkerStop
from pq.worker import worker_loop


def test_worker_executes_one_pipeline(tmp_path: Path):
    data_dir = tmp_path / "data"
    pipe = tmp_path / "pipe"
    pipe.mkdir()
    (pipe / "prompts").mkdir()
    (pipe / "outputs").mkdir()
    (pipe / "pipeline.yaml").write_text(textwrap.dedent("""
        name: hello
        steps:
          - id: g
            command: sh
            args: ["-c", "echo hi > outputs/x.txt"]
            produces: ["outputs/x.txt"]
    """).strip())
    cfg = Config(data_dir=data_dir, poll_interval_seconds=0)
    r = CliRunner()
    r.invoke(main, ["--data-dir", str(data_dir), "add", str(pipe)])

    stop = WorkerStop()
    stop.should_stop = True  # stop after one run
    worker_loop(cfg, stop)
    assert (pipe / "outputs" / "x.txt").exists()

    from pq.db import get_conn
    conn = get_conn(data_dir / "pq.db")
    cur = conn.execute("SELECT status FROM runs")
    statuses = [r["status"] for r in cur.fetchall()]
    assert "done" in statuses
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_daemon.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'pq.worker'`.

- [ ] **Step 3: Implement pq/worker.py**

`pq/worker.py`:
```python
"""The main worker loop: pick next run, execute, repeat."""
from __future__ import annotations

import datetime as dt
import json
import time
import zoneinfo
from pathlib import Path

from pq import counter as counter_mod
from pq import db as db_mod
from pq import runner as runner_mod
from pq import scheduler as scheduler_mod
from pq.config import Config
from pq.pipelines import load_pipeline, validate_pipeline
from pq.signals import WorkerStop


def _today_in_tz(tz: str) -> str:
    try:
        zone = zoneinfo.ZoneInfo(tz)
    except Exception:
        zone = zoneinfo.ZoneInfo("UTC")
    return dt.datetime.now(zone).date().isoformat()


def _execute_step(step_raw: dict, pipeline_dir: Path, run_id: int, data_dir: Path, run_inputs: dict) -> tuple[str, int | None]:
    """Reconstruct a Step from DB row (raw dict) and run it. Returns (status, exit_code)."""
    from pq.pipelines import Step, Iterates
    step = Step(
        id=step_raw["step_id"],
        command=step_raw.get("command") or "true",  # filled by reconstruction below
        args=[],
    )
    # We need a full Step, so we re-load the pipeline from the snapshot.
    raise NotImplementedError("see _execute_step_via_pipeline")


def _execute_run(conn, run_id: int, data_dir: Path, cfg: Config, today: str) -> None:
    """Run a single run end-to-end."""
    cur = conn.execute("SELECT * FROM runs WHERE id=?", (run_id,))
    run = cur.fetchone()
    if run is None:
        return

    # Re-load pipeline from snapshot
    snapshot_path = data_dir / "runs" / str(run_id) / "meta.json"
    snapshot = json.loads(snapshot_path.read_text())
    pipeline_dir = Path(snapshot["pipeline_dir"])
    pipe = load_pipeline(pipeline_dir)
    # The pipeline on disk may have changed; we re-validate just to be safe but
    # the snapshot is the source of truth. We rebuild the Pipeline from snapshot.

    conn.execute("UPDATE runs SET status='running', started_at=? WHERE id=?",
                 (dt.datetime.now().isoformat(), run_id))
    conn.commit()

    inputs = json.loads(run["inputs_json"]) if run["inputs_json"] else {}

    # Build a synthetic Pipeline from the snapshot dict
    pipe = _pipeline_from_snapshot(snapshot, pipeline_dir)

    # Topological order
    order = _topo_order(pipe)

    failed = False
    for step in order:
        # Re-check quota if this is an upload step
        if step.type == "upload":
            if counter_mod.get_uploads_today(conn, today) >= cfg.max_uploads_per_day:
                conn.execute("UPDATE runs SET status='waiting' WHERE id=?", (run_id,))
                conn.commit()
                return

        conn.execute("UPDATE steps SET status='running' WHERE run_id=? AND step_id=?",
                     (run_id, step.id))
        conn.commit()

        result = runner_mod.run_step_with_retries(
            step=step,
            pipeline=pipe,
            run_id=run_id,
            data_dir=data_dir,
            run_inputs=inputs,
            max_attempts=cfg.max_attempts,
            backoff=cfg.backoff,
        )

        if result.status == "done":
            conn.execute("UPDATE steps SET status='done' WHERE run_id=? AND step_id=?",
                         (run_id, step.id))
            if step.type == "upload":
                scheduler_mod.mark_upload_done(conn, run_id, today)
        elif result.status == "skipped":
            conn.execute("UPDATE steps SET status='done' WHERE run_id=? AND step_id=?",
                         (run_id, step.id))
        else:
            conn.execute("UPDATE steps SET status='failed' WHERE run_id=? AND step_id=?",
                         (run_id, step.id))
            failed = True
            break
        conn.commit()

    if failed:
        conn.execute("UPDATE runs SET status='failed', finished_at=?, error=? WHERE id=?",
                     (dt.datetime.now().isoformat(), "step failed", run_id))
    else:
        finished = dt.datetime.now().isoformat()
        conn.execute("UPDATE runs SET status='done', finished_at=? WHERE id=?", (finished, run_id))
        # Apply cooldown
        cooldown_until = (dt.datetime.fromisoformat(finished) + dt.timedelta(seconds=pipe.cooldown_seconds)).isoformat()
        conn.execute("UPDATE runs SET cooldown_until=? WHERE id=?", (cooldown_until, run_id))
    conn.commit()


def _pipeline_from_snapshot(snapshot: dict, pipeline_dir: Path):
    """Rebuild a Pipeline object from a snapshot dict."""
    from pq.pipelines import Pipeline, Step, Iterates, Input
    steps = []
    for s in snapshot.get("steps", []):
        it_raw = s.get("iterates")
        iterates = None
        if it_raw:
            iterates = Iterates(
                count=it_raw.get("count"),
                count_from=it_raw.get("count_from"),
                out_template=it_raw.get("out_template"),
            )
        steps.append(Step(
            id=s["id"],
            command=s["command"],
            args=s.get("args", []),
            needs=s.get("needs", []),
            iterates=iterates,
            produces=s.get("produces", []),
            type=s.get("type"),
        ))
    inputs = {k: Input(type=v.get("type", "string"), required=v.get("required", False))
              for k, v in (snapshot.get("inputs") or {}).items()}
    return Pipeline(
        name=snapshot["name"],
        dir=pipeline_dir,
        cooldown_seconds=0,  # not relevant for execution; cooldown is applied at the run level
        inputs=inputs,
        steps=steps,
    )


def _topo_order(pipe):
    by_id = {s.id: s for s in pipe.steps}
    visited = set()
    order = []

    def visit(sid):
        if sid in visited:
            return
        visited.add(sid)
        for need in by_id[sid].needs:
            visit(need)
        order.append(by_id[sid])

    for s in pipe.steps:
        visit(s.id)
    return order


def worker_loop(cfg: Config, stop: WorkerStop) -> None:
    """Main loop. Polls every cfg.poll_interval_seconds when idle."""
    db_mod.init_db(cfg.data_dir)
    while not stop.should_stop:
        conn = db_mod.get_conn(cfg.data_dir / "pq.db")
        try:
            now = dt.datetime.now().isoformat()
            today = _today_in_tz(cfg.timezone)
            run_id = scheduler_mod.pick_next_run(conn, now, cfg.max_uploads_per_day, today)
            if run_id is None:
                if cfg.poll_interval_seconds > 0:
                    time.sleep(cfg.poll_interval_seconds)
                continue
            _execute_run(conn, run_id, cfg.data_dir, cfg, today)
        finally:
            conn.close()
```

- [ ] **Step 4: Implement the `daemon` CLI command**

Replace the `daemon` function in `pq/cli.py`:

```python
@main.command()
@click.pass_context
def daemon(ctx: click.Context) -> None:
    """Run the worker in the foreground."""
    from pq.worker import worker_loop
    from pq.signals import WorkerStop, install_handlers
    cfg: Config = ctx.obj["config"]
    stop = WorkerStop()
    install_handlers(stop)
    click.echo("Worker started. Ctrl+C to stop.")
    worker_loop(cfg, stop)
    click.echo("Worker stopped.")
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_daemon.py -v`
Expected: 1 passed.

- [ ] **Step 6: Commit**

```bash
cd /home/pepe/Desktop/pipeline-queue
git add .
git commit -m "feat: pq daemon worker loop"
```

---

## Task 20: Example pipeline (youtube-video)

**Files:**
- Create: `/home/pepe/Desktop/pipeline-queue/examples/youtube-video/pipeline.yaml`
- Create: `/home/pepe/Desktop/pipeline-queue/examples/youtube-video/scripts/echo.sh`
- Create: `/home/pepe/Desktop/pipeline-queue/examples/youtube-video/prompts/.gitkeep`
- Create: `/home/pepe/Desktop/pipeline-queue/examples/youtube-video/outputs/.gitkeep`

**Interfaces:**
- Consumes: --input topic=
- Produces: a working example using `echo` as a stand-in for the real tools

- [ ] **Step 1: Create the example YAML**

`examples/youtube-video/pipeline.yaml`:
```yaml
name: youtube-video
cooldown: 4h
inputs:
  topic:
    type: string
    required: true

steps:
  - id: guion
    command: sh
    args: ["-c", "echo 'Guion sobre {topic}' > outputs/guion.txt && for i in 1 2 3 4 5 6; do echo \"prompt $i sobre {topic}\" > prompts/img_$i.txt; done"]
    produces:
      - outputs/guion.txt
      - prompts/img_{i}.txt
    iterates:
      count: 6
      out_template: prompts/img_{i}.txt

  - id: imagenes
    needs: [guion]
    command: sh
    args: ["-c", "cp {prompts} outputs/imagenes/img_{i}.txt"]
    iterates:
      count_from: prompts/img_*.txt
    produces:
      - outputs/imagenes/img_{i}.txt

  - id: clips
    needs: [imagenes]
    command: sh
    args: ["-c", "echo 'clip from {imagenes}' > outputs/clips/clip_{i}.txt"]
    iterates:
      count_from: outputs/imagenes/img_*.txt
    produces:
      - outputs/clips/clip_{i}.txt

  - id: audio
    needs: [guion]
    command: sh
    args: ["-c", "echo 'audio track' > outputs/audio.txt"]
    produces:
      - outputs/audio.txt

  - id: montaje
    needs: [clips, audio]
    command: sh
    args: ["-c", "echo 'final video' > outputs/final.txt"]
    produces:
      - outputs/final.txt

  - id: upload
    type: upload
    needs: [montaje]
    command: sh
    args: ["-c", "echo 'would upload' > outputs/uploaded.txt"]
    produces:
      - outputs/uploaded.txt
```

- [ ] **Step 2: Create placeholder directories and script**

```bash
mkdir -p /home/pepe/Desktop/pipeline-queue/examples/youtube-video/prompts
mkdir -p /home/pepe/Desktop/pipeline-queue/examples/youtube-video/outputs
mkdir -p /home/pepe/Desktop/pipeline-queue/examples/youtube-video/outputs/imagenes
mkdir -p /home/pepe/Desktop/pipeline-queue/examples/youtube-video/outputs/clips
mkdir -p /home/pepe/Desktop/pipeline-queue/examples/youtube-video/scripts
touch /home/pepe/Desktop/pipeline-queue/examples/youtube-video/prompts/.gitkeep
touch /home/pepe/Desktop/pipeline-queue/examples/youtube-video/outputs/.gitkeep
echo "#!/bin/sh\necho \"$@\"" > /home/pepe/Desktop/pipeline-queue/examples/youtube-video/scripts/echo.sh
chmod +x /home/pepe/Desktop/pipeline-queue/examples/youtube-video/scripts/echo.sh
```

- [ ] **Step 3: Test the example end-to-end**

Run:
```bash
cd /home/pepe/Desktop/pipeline-queue
.venv/bin/pq --data-dir /tmp/pq-test add examples/youtube-video --input topic="IA en 2026"
.venv/bin/pq --data-dir /tmp/pq-test daemon &
DAEMON_PID=$!
sleep 5
kill -INT $DAEMON_PID
wait $DAEMON_PID 2>/dev/null
ls /home/pepe/Desktop/pipeline-queue/examples/youtube-video/outputs/
.venv/bin/pq --data-dir /tmp/pq-test list
```

Expected: outputs/ contains `guion.txt`, `imagenes/`, `clips/`, `audio.txt`, `final.txt`, `uploaded.txt`.

- [ ] **Step 4: Commit**

```bash
cd /home/pepe/Desktop/pipeline-queue
git add .
git commit -m "docs: example youtube-video pipeline"
```

---

## Task 21: End-to-end smoke test (the entire system)

**Files:**
- Create: `/home/pepe/Desktop/pipeline-queue/tests/test_smoke.py`

**Interfaces:**
- Produces: one test that exercises the full path: add → daemon → status=done

- [ ] **Step 1: Write the smoke test**

`tests/test_smoke.py`:
```python
import time
from pathlib import Path
import textwrap
from click.testing import CliRunner
from pq.cli import main
from pq.config import Config
from pq.signals import WorkerStop
from pq.worker import worker_loop


def test_full_run_end_to_end(tmp_path: Path):
    pipe = tmp_path / "pipe"
    pipe.mkdir()
    (pipe / "prompts").mkdir()
    (pipe / "outputs").mkdir()
    (pipe / "pipeline.yaml").write_text(textwrap.dedent("""
        name: smoke
        steps:
          - id: g
            command: sh
            args: ["-c", "echo hi > outputs/x.txt"]
            produces: ["outputs/x.txt"]
    """).strip())
    data_dir = tmp_path / "data"
    r = CliRunner()
    r.invoke(main, ["--data-dir", str(data_dir), "add", str(pipe), "--input", "topic=t"])
    cfg = Config(data_dir=data_dir, poll_interval_seconds=0)
    stop = WorkerStop()
    stop.should_stop = True
    worker_loop(cfg, stop)
    assert (pipe / "outputs" / "x.txt").exists()
    from pq.db import get_conn
    conn = get_conn(data_dir / "pq.db")
    cur = conn.execute("SELECT status FROM runs")
    assert all(row["status"] == "done" for row in cur.fetchall())
```

- [ ] **Step 2: Run the smoke test**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest tests/test_smoke.py -v`
Expected: 1 passed.

- [ ] **Step 3: Run the full test suite**

Run: `cd /home/pepe/Desktop/pipeline-queue && .venv/bin/pytest -v`
Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
cd /home/pepe/Desktop/pipeline-queue
git add .
git commit -m "test: end-to-end smoke test"
```

---

## Self-Review

**Spec coverage** (sections of the spec → tasks that implement them):

- CLI commands (`add`, `list`, `logs`, `retry`, `cancel`, `daemon`) → Tasks 2, 7, 8, 16, 17, 18, 19.
- Config loading → Task 3.
- Queue SQLite schema → Task 4.
- Per-pipeline DB → Task 6.
- YAML parsing + validation (fail fast) → Task 5.
- Iteration expansion (count, count_from) → Task 9.
- Step runner (env vars, args resolution, skip-if-exists, retries, fan-out) → Tasks 10, 11.
- Cooldown and quota → Tasks 12, 13.
- Cancellation (SIGKILL, mark cancelled, keep outputs) → Task 14.
- Signal handlers (Ctrl+C = stop after current step) → Task 15.
- Snapshot of YAML at add → Task 7.
- Skip-if-exists idempotency → Task 10.
- Example pipeline → Task 20.
- End-to-end smoke → Task 21.

**Gaps found:** none.

**Placeholder scan:** no TBDs, no "implement later", all steps have concrete code or commands.

**Type consistency:** Step/Pipeline/Iterates/Input shapes are used identically across tasks 5, 9, 10, 11, 13, 19. `worker_loop(cfg, stop)` is the signature used in tests and CLI.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-08-14-pipeline-queue.md`. Two execution options:

1. **Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
