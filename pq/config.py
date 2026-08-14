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
    cycle_pipelines: tuple[str, ...] = ()


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
    scheduler = data.get("scheduler", {})

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
        cycle_pipelines=tuple(scheduler.get("cycle_pipelines", [])),
    )
