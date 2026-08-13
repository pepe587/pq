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
