"""CLI entry point for pipeline-queue."""
from __future__ import annotations

import click

from pq import config as config_mod
from pq import db as db_mod
from pq import pipelines as pipelines_mod
from pq import runner as runner_mod
from pq import scheduler as scheduler_mod
from pq import cancel as cancel_mod


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
    raise NotImplementedError


@main.command(name="list")
@click.option("--status", default=None, help="Filter by status.")
@click.pass_context
def list_cmd(ctx: click.Context, status: str | None) -> None:
    """List runs."""
    raise NotImplementedError


@main.command()
@click.argument("run_id", type=int)
@click.argument("step_id", required=False)
@click.pass_context
def logs(ctx: click.Context, run_id: int, step_id: str | None) -> None:
    """Show logs of a run or step."""
    raise NotImplementedError


@main.command()
@click.argument("run_id", type=int)
@click.pass_context
def retry(ctx: click.Context, run_id: int) -> None:
    """Retry a failed run."""
    raise NotImplementedError


@main.command()
@click.argument("run_id", type=int)
@click.pass_context
def cancel(ctx: click.Context, run_id: int) -> None:
    """Cancel a run."""
    raise NotImplementedError


@main.command()
@click.pass_context
def daemon(ctx: click.Context) -> None:
    """Run the worker in the foreground."""
    raise NotImplementedError


if __name__ == "__main__":
    main()
