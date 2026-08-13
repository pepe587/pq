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
