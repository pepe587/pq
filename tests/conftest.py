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
