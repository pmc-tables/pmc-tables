"""Tests for `pmc_tables` package."""
import pytest
from click.testing import CliRunner

import pmc_tables
from pmc_tables import cli


@pytest.mark.parametrize('attribute', ['__author__', '__email__', '__version__'])
def test_attribute(attribute):
    assert getattr(pmc_tables, attribute)


def test_main():
    from pmc_tables import pmc_tables
    assert pmc_tables


def test_command_line_interface():
    """Test the CLI."""
    runner = CliRunner()
    # result = runner.invoke(cli.main)
    # assert result.exit_code == 0
    # assert 'pmc_tables.cli.main' in result.output
    help_result = runner.invoke(cli.main, ['--help'])
    assert help_result.exit_code == 0
    assert 'Show this message and exit.' in help_result.output
