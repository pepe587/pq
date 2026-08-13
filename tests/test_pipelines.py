from pathlib import Path
import textwrap
import pytest
from pq.pipelines import load_pipeline, validate_pipeline, PipelineError


def write_pipeline(d: Path, yaml: str) -> Path:
    (d / "pipeline.yaml").write_text(textwrap.dedent(yaml).strip())
    return d


def test_load_minimal_pipeline(tmp_pipeline_dir: Path):
    write_pipeline(tmp_pipeline_dir, """
        name: hello
        steps:
          - id: greet
            command: echo
            args: ["hi"]
    """)
    p = load_pipeline(tmp_pipeline_dir)
    assert p.name == "hello"
    assert p.cooldown_seconds == 0
    assert len(p.steps) == 1
    assert p.steps[0].id == "greet"
    assert p.steps[0].command == "echo"
    assert p.steps[0].args == ["hi"]
    assert p.steps[0].needs == []
    assert p.steps[0].iterates is None


def test_load_full_pipeline(tmp_pipeline_dir: Path):
    write_pipeline(tmp_pipeline_dir, """
        name: youtube
        cooldown: 4h
        inputs:
          topic:
            type: string
            required: true
        steps:
          - id: guion
            command: ollama
            args: ["run", "cloud", "{topic}"]
            iterates:
              count: 6
              out_template: prompts/img_{i}.txt
            produces:
              - prompts/img_{i}.txt
          - id: imagenes
            needs: [guion]
            command: ideogram
            args: ["--prompt-file", "{prompts}"]
            iterates:
              count_from: prompts/img_*.txt
            produces:
              - outputs/imagenes/img_{i}.png
          - id: upload
            type: upload
            needs: [imagenes]
            command: yt-upload
            args: ["--video", "outputs/final.mp4"]
    """)
    p = load_pipeline(tmp_pipeline_dir)
    assert p.cooldown_seconds == 4 * 3600
    assert p.inputs["topic"].required is True
    assert p.steps[0].iterates.count == 6
    assert p.steps[0].iterates.out_template == "prompts/img_{i}.txt"
    assert p.steps[1].needs == ["guion"]
    assert p.steps[1].iterates.count_from == "prompts/img_*.txt"
    assert p.steps[2].type == "upload"


def test_validate_unknown_need_raises(tmp_pipeline_dir: Path):
    write_pipeline(tmp_pipeline_dir, """
        name: bad
        steps:
          - id: a
            command: echo
            needs: [nonexistent]
    """)
    p = load_pipeline(tmp_pipeline_dir)
    with pytest.raises(PipelineError, match="nonexistent"):
        validate_pipeline(p)


def test_validate_duplicate_step_id_raises(tmp_pipeline_dir: Path):
    write_pipeline(tmp_pipeline_dir, """
        name: bad
        steps:
          - id: a
            command: echo
          - id: a
            command: echo
    """)
    p = load_pipeline(tmp_pipeline_dir)
    with pytest.raises(PipelineError, match="duplicate"):
        validate_pipeline(p)


def test_validate_cycle_raises(tmp_pipeline_dir: Path):
    write_pipeline(tmp_pipeline_dir, """
        name: bad
        steps:
          - id: a
            command: echo
            needs: [b]
          - id: b
            command: echo
            needs: [a]
    """)
    p = load_pipeline(tmp_pipeline_dir)
    with pytest.raises(PipelineError, match="cycle"):
        validate_pipeline(p)


def test_validate_iterates_needs_count_or_countfrom(tmp_pipeline_dir: Path):
    write_pipeline(tmp_pipeline_dir, """
        name: bad
        steps:
          - id: a
            command: echo
            iterates: {}
    """)
    p = load_pipeline(tmp_pipeline_dir)
    with pytest.raises(PipelineError, match="iterates"):
        validate_pipeline(p)


def test_validate_iterates_both_count_and_countfrom_raises(tmp_pipeline_dir: Path):
    write_pipeline(tmp_pipeline_dir, """
        name: bad
        steps:
          - id: a
            command: echo
            iterates:
              count: 3
              count_from: foo/*.txt
    """)
    p = load_pipeline(tmp_pipeline_dir)
    with pytest.raises(PipelineError, match="iterates"):
        validate_pipeline(p)
