from __future__ import annotations

import click
import pytest
from click.testing import CliRunner

from metricflow.cli.custom_click_types import MutuallyExclusiveOption, SequenceParamType


def test_check_min_length() -> None:
    """Make sure `SequenceParamType` checks for `min_length` param."""
    ltype = SequenceParamType[str](min_length=2)
    ltype.convert("1,2", None, None)
    with pytest.raises(click.BadParameter):
        ltype.convert("1", None, None)


def test_check_max_length() -> None:
    """Make sure `SequenceParamType` checks for `max_length` param."""
    ltype = SequenceParamType[str](max_length=2)
    ltype.convert("1,2", None, None)
    with pytest.raises(click.BadParameter):
        ltype.convert("1,2,3", None, None)


def test_use_value_converter() -> None:
    """Make sure `SequenceParamType` uses `value_converter`."""
    ltype = SequenceParamType[int](value_converter=lambda x_str: int(x_str))
    values = ltype.convert("1,2,3,4,5", None, None)

    assert values == [1, 2, 3, 4, 5]


def test_value_converter_fail() -> None:
    """Make sure `SequenceParamType` fails gracefully when a value cannot be converted."""
    ltype = SequenceParamType[int](value_converter=lambda x_str: int(x_str))
    with pytest.raises(click.BadParameter):
        ltype.convert("1,abc", None, None)


def test_use_separator() -> None:
    """Make sure `SequenceParamType` uses `separator`."""
    ltype = SequenceParamType[str](separator=":")

    sep_values = ltype.convert("1:2:3:4:5", None, None)
    assert sep_values == ["1", "2", "3", "4", "5"]

    no_sep_values = ltype.convert("1,2,3,4,5", None, None)
    assert no_sep_values == ["1,2,3,4,5"]


def test_mutually_exclusive_option() -> None:
    """Make sure `MutuallyExclusiveOption` works."""

    @click.command()
    @click.option(
        "--foo",
        cls=MutuallyExclusiveOption,
        mutually_exclusive=["bar"],
        help="",
    )
    @click.option(
        "--bar",
        cls=MutuallyExclusiveOption,
        mutually_exclusive=["foo"],
        help="",
    )
    def test(foo: str, bar: str) -> None:
        pass

    runner = CliRunner()

    # should work when exclusivity is respected
    result = runner.invoke(test, ["--foo", "abc"])
    assert result.exit_code == 0
    result = runner.invoke(test, ["--bar", "abc"])
    assert result.exit_code == 0

    # should fail when it is not respected
    result = runner.invoke(test, ["--foo", "abc", "--bar", "abc"])
    assert result.exit_code != 0
