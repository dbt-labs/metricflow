import pytest
import click

from metricflow.cli.custom_click_types import ListParamType


def test_check_min_length():
    """Make sure `ListParamType` checks for `min_length` param."""
    ltype = ListParamType(min_length=2)
    ltype.convert("1,2", None, None)
    with pytest.raises(click.BadParameter):
        ltype.convert("1", None, None)


def test_check_max_length():
    """Make sure `ListParamType` checks for `max_length` param."""
    ltype = ListParamType(max_length=2)
    ltype.convert("1,2", None, None)
    with pytest.raises(click.BadParameter):
        ltype.convert("1,2,3", None, None)


def test_use_value_converter():
    """Make sure `ListParamType` uses `value_converter`."""
    ltype = ListParamType(value_converter=lambda x_str: int(x_str))
    values = ltype.convert("1,2,3,4,5", None, None)

    assert values == [1, 2, 3, 4, 5]


def test_value_converter_fail():
    """Make sure `ListParamType` fails gracefully when a value cannot be converted."""
    ltype = ListParamType(value_converter=lambda x_str: int(x_str))
    with pytest.raises(click.BadParameter):
        ltype.convert("1,abc", None, None)


def test_use_separator():
    """Make sure `ListParamType` uses `separator`."""
    ltype = ListParamType(separator=":")

    sep_values = ltype.convert("1:2:3:4:5", None, None)
    assert sep_values == ["1", "2", "3", "4", "5"]

    no_sep_values = ltype.convert("1,2,3,4,5", None, None)
    assert no_sep_values == ["1,2,3,4,5"]
