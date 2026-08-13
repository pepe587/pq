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
