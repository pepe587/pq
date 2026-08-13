"""CLI entry point for pipeline-queue."""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import click
import yaml as yaml_lib

from pq import config as config_mod
from pq import db as db_mod
from pq import pipeline_db as pipeline_db_mod
from pq import pipelines as pipelines_mod
from pq import runner as runner_mod
from pq import scheduler as scheduler_mod
from pq import cancel as cancel_mod
from pq.config import Config


@click.group()
@click.option("--config", "config_path", default=None, help="Path to config.toml.")
@click.option("--data-dir", "data_dir", default=None, help="Override data dir.")
@click.pass_context
def main(ctx: click.Context, config_path: str | None, data_dir: str | None) -> None:
    """pipeline-queue: FIFO executor for declarative YAML pipelines."""
    cfg = config_mod.load_config(config_path, data_dir)
    ctx.ensure_object(dict)
    ctx.obj["config"] = cfg


@main.command()
@click.argument("pipeline_dir", type=click.Path(exists=True, file_okay=False))
@click.option("--input", "inputs", multiple=True, help="key=value (can repeat).")
@click.pass_context
def add(ctx: click.Context, pipeline_dir: str, inputs: tuple[str, ...]) -> None:
    """Enqueue a new pipeline run."""
    cfg: Config = ctx.obj["config"]
    pipe_path = Path(pipeline_dir).resolve()
    try:
        pipe = pipelines_mod.load_pipeline(pipe_path)
        pipelines_mod.validate_pipeline(pipe)
    except pipelines_mod.PipelineError as e:
        raise click.ClickException(f"invalid pipeline: {e}")

    # Parse inputs
    provided: dict[str, str] = {}
    for raw in inputs:
        if "=" not in raw:
            raise click.ClickException(f"--input must be key=value, got {raw!r}")
        k, v = raw.split("=", 1)
        provided[k] = v
    for name, spec in pipe.inputs.items():
        if spec.required and name not in provided:
            raise click.ClickException(f"missing required input: {name}")

    db_mod.init_db(cfg.data_dir)

    # Compute cooldown_until from previous successful runs of this pipeline
    conn = db_mod.get_conn(cfg.data_dir / "pq.db")
    try:
        cur = conn.execute(
            "SELECT finished_at FROM runs WHERE pipeline_name=? AND status='done' ORDER BY finished_at DESC LIMIT 1",
            (pipe.name,),
        )
        row = cur.fetchone()
        cooldown_until = None
        if row and row["finished_at"]:
            finished = dt.datetime.fromisoformat(row["finished_at"])
            cooldown_until = (finished + dt.timedelta(seconds=pipe.cooldown_seconds)).isoformat()

        # Snapshot the YAML
        run_id_row = conn.execute(
            "INSERT INTO runs (pipeline_name, pipeline_dir, inputs_json, status, created_at, cooldown_until) "
            "VALUES (?, ?, ?, 'queued', ?, ?)",
            (
                pipe.name,
                str(pipe_path),
                json.dumps(provided),
                dt.datetime.now().isoformat(),
                cooldown_until,
            ),
        )
        run_id = run_id_row.lastrowid
        if run_id is None:
            raise click.ClickException("failed to insert run")

        for s in pipe.steps:
            conn.execute(
                "INSERT INTO steps (run_id, step_id, needs_json, iterates_json, produces_json, type, status) "
                "VALUES (?, ?, ?, ?, ?, ?, 'pending')",
                (
                    run_id,
                    s.id,
                    json.dumps(s.needs),
                    json.dumps({
                        "count": s.iterates.count if s.iterates else None,
                        "count_from": s.iterates.count_from if s.iterates else None,
                        "out_template": s.iterates.out_template if s.iterates else None,
                    }),
                    json.dumps(s.produces),
                    s.type,
                ),
            )

        # Persist snapshot of the YAML + meta
        with (pipe_path / "pipeline.yaml").open() as f:
            yaml_text = f.read()
        snapshot = yaml_lib.safe_load(yaml_text)
        run_dir = cfg.data_dir / "runs" / str(run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        meta = {
            "run_id": run_id,
            "pipeline_name": pipe.name,
            "pipeline_dir": str(pipe_path),
            "inputs": provided,
            "created_at": dt.datetime.now().isoformat(),
            "snapshot": snapshot,
        }
        (run_dir / "meta.json").write_text(json.dumps(meta, indent=2))

        conn.commit()
    finally:
        conn.close()

    # Ensure per-pipeline DB exists
    pipeline_db_mod.ensure_pipeline_db(cfg.data_dir, pipe.name)

    click.echo(f"Run {run_id} queued: {pipe.name}")


@main.command(name="list")
@click.option("--status", default=None, help="Filter by status.")
@click.option("--limit", default=20, type=int, help="Max runs to show.")
@click.pass_context
def list_cmd(ctx: click.Context, status: str | None, limit: int) -> None:
    """List runs."""
    cfg: Config = ctx.obj["config"]
    db_path = db_mod.init_db(cfg.data_dir)
    conn = db_mod.get_conn(db_path)
    try:
        if status:
            cur = conn.execute(
                "SELECT id, pipeline_name, status, created_at, finished_at "
                "FROM runs WHERE status=? ORDER BY id DESC LIMIT ?",
                (status, limit),
            )
        else:
            cur = conn.execute(
                "SELECT id, pipeline_name, status, created_at, finished_at "
                "FROM runs ORDER BY id DESC LIMIT ?",
                (limit,),
            )
        rows = cur.fetchall()
        if not rows:
            click.echo("(no runs)")
            return
        click.echo(f"{'ID':<6} {'PIPELINE':<20} {'STATUS':<12} {'CREATED':<20} {'FINISHED':<20}")
        for r in rows:
            click.echo(
                f"{r['id']:<6} {r['pipeline_name']:<20} {r['status']:<12} "
                f"{(r['created_at'] or ''):<20} {(r['finished_at'] or ''):<20}"
            )
    finally:
        conn.close()


@main.command()
@click.argument("run_id", type=int)
@click.argument("step_id", required=False)
@click.pass_context
def logs(ctx: click.Context, run_id: int, step_id: str | None) -> None:
    """Show logs of a run or step."""
    cfg: Config = ctx.obj["config"]
    base = cfg.data_dir / "runs" / str(run_id) / "steps"
    if not base.exists():
        raise click.ClickException(f"no logs for run {run_id}")
    if step_id:
        step_dirs = sorted(base.glob(f"{step_id}/*"))
        if not step_dirs:
            raise click.ClickException(f"no logs for step {step_id}")
        for sd in step_dirs:
            log = sd / "log.txt"
            if log.exists():
                click.echo(f"=== {sd.parent.name}/{sd.name} ===")
                click.echo(log.read_text(), nl=False)
    else:
        for sd in sorted(base.glob("*/*")):
            log = sd / "log.txt"
            if log.exists():
                click.echo(f"=== {sd.parent.name}/{sd.name} ===")
                click.echo(log.read_text(), nl=False)


@main.command()
@click.argument("run_id", type=int)
@click.pass_context
def retry(ctx: click.Context, run_id: int) -> None:
    """Retry a failed run (resets failed steps to pending, marks run queued)."""
    cfg: Config = ctx.obj["config"]
    db_path = db_mod.init_db(cfg.data_dir)
    conn = db_mod.get_conn(db_path)
    try:
        cur = conn.execute("SELECT status FROM runs WHERE id=?", (run_id,))
        row = cur.fetchone()
        if row is None:
            raise click.ClickException(f"no such run: {run_id}")
        if row["status"] != "failed":
            raise click.ClickException(f"run {run_id} is not failed (status: {row['status']})")
        conn.execute("UPDATE runs SET status='queued' WHERE id=?", (run_id,))
        conn.execute(
            "UPDATE steps SET status='pending', attempts=0 WHERE run_id=? AND status='failed'",
            (run_id,),
        )
        conn.commit()
        click.echo(f"Run {run_id} requeued")
    finally:
        conn.close()


@main.command()
@click.argument("run_id", type=int)
@click.pass_context
def cancel(ctx: click.Context, run_id: int) -> None:
    """Cancel a run."""
    cfg: Config = ctx.obj["config"]
    db_path = db_mod.init_db(cfg.data_dir)
    conn = db_mod.get_conn(db_path)
    try:
        cancel_mod.cancel_run(conn, run_id, cfg.data_dir)
        click.echo(f"Run {run_id} cancelled")
    finally:
        conn.close()


@main.command()
@click.pass_context
def daemon(ctx: click.Context) -> None:
    """Run the worker in the foreground."""
    raise NotImplementedError


if __name__ == "__main__":
    main()
