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
    for i, topic in enumerate(["a", "b"]):
        pipe = tmp_path / f"pipe{i}"
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
