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
