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
