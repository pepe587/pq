from pathlib import Path
from pq.pipelines import Step, Iterates
from pq.iterations import expand_iterations


def test_no_iterates_returns_one(tmp_path: Path):
    step = Step(id="a", command="echo", args=["hi"])
    iters = expand_iterations(step, tmp_path)
    assert len(iters) == 1
    assert iters[0].index == 1


def test_count_expands(tmp_path: Path):
    step = Step(
        id="a",
        command="echo",
        produces=["outputs/img_{i}.png"],
        iterates=Iterates(count=3, out_template="prompts/img_{i}.txt"),
    )
    iters = expand_iterations(step, tmp_path)
    assert [it.index for it in iters] == [1, 2, 3]
    assert iters[0].substituted_outputs == {"outputs/img_1.png": tmp_path / "outputs" / "img_1.png"}
    assert iters[2].substituted_outputs == {"outputs/img_3.png": tmp_path / "outputs" / "img_3.png"}


def test_count_from_glob(tmp_path: Path):
    (tmp_path / "prompts").mkdir()
    for i in range(1, 4):
        (tmp_path / "prompts" / f"img_{i}.txt").write_text(f"prompt {i}")
    step = Step(
        id="a",
        command="ideogram",
        iterates=Iterates(count_from="prompts/img_*.txt"),
    )
    iters = expand_iterations(step, tmp_path)
    assert len(iters) == 1
    assert iters[0].index == 1
    assert iters[0].matched_glob == [
        tmp_path / "prompts" / "img_1.txt",
        tmp_path / "prompts" / "img_2.txt",
        tmp_path / "prompts" / "img_3.txt",
    ]


def test_count_from_glob_zero_files_raises(tmp_path: Path):
    (tmp_path / "prompts").mkdir()
    step = Step(
        id="a",
        command="ideogram",
        iterates=Iterates(count_from="prompts/img_*.txt"),
    )
    import pytest
    with pytest.raises(ValueError, match="no files"):
        expand_iterations(step, tmp_path)
