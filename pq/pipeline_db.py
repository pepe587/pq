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
