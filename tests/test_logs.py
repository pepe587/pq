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
