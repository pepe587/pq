from click.testing import CliRunner
from pq.cli import main


def test_cli_runs():
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "Usage:" in result.output


def test_cli_has_add_subcommand():
    runner = CliRunner()
    result = runner.invoke(main, ["add", "--help"])
    assert result.exit_code == 0
    assert "--input" in result.output
