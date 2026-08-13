import os
import time
from pathlib import Path
import textwrap
from click.testing import CliRunner
from pq.cli import main
from pq.pipelines import Pipeline, Step, Iterates, Input
from pq.runner import run_step, StepResult, run_step_with_retries


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


def test_fan_out_count(tmp_path: Path):
    pipe = tmp_path / "pipe"
    pipe.mkdir()
    (pipe / "prompts").mkdir()
    (pipe / "prompts" / "img_1.txt").write_text("p1")
    (pipe / "prompts" / "img_2.txt").write_text("p2")
    (pipe / "prompts" / "img_3.txt").write_text("p3")
    (pipe / "outputs" / "imagenes").mkdir(parents=True)
    p = make_pipeline("p", pipe)
    step = Step(
        id="imgs",
        command="sh",
        args=["-c", "cp {prompts} outputs/imagenes/img_{i}.txt"],
        produces=["outputs/imagenes/img_{i}.txt"],
        iterates=Iterates(count_from="prompts/img_*.txt"),
    )
    result = run_step_with_retries(
        step=step,
        pipeline=p,
        run_id=1,
        data_dir=tmp_path / "data",
        run_inputs={},
        max_attempts=3,
        backoff=(0, 0, 0),
    )
    assert result.status == "done"
    for i in (1, 2, 3):
        assert (pipe / "outputs" / "imagenes" / f"img_{i}.txt").exists()


def test_retry_then_success(tmp_path: Path):
    pipe = tmp_path / "pipe"
    pipe.mkdir()
    p = make_pipeline("p", pipe)
    # Use a counter file to fail twice then succeed
    (pipe / "counter").write_text("0")
    step = Step(
        id="a",
        command="sh",
        args=["-c", "n=$(cat counter); n=$((n+1)); echo $n > counter; [ $n -ge 3 ]"],
    )
    result = run_step_with_retries(
        step=step,
        pipeline=p,
        run_id=1,
        data_dir=tmp_path / "data",
        run_inputs={},
        max_attempts=5,
        backoff=(0, 0, 0, 0, 0),
    )
    assert result.status == "done"


def test_retry_exhausted_returns_failed(tmp_path: Path):
    pipe = tmp_path / "pipe"
    pipe.mkdir()
    p = make_pipeline("p", pipe)
    step = Step(id="a", command="false")
    result = run_step_with_retries(
        step=step,
        pipeline=p,
        run_id=1,
        data_dir=tmp_path / "data",
        run_inputs={},
        max_attempts=3,
        backoff=(0, 0, 0),
    )
    assert result.status == "failed"
