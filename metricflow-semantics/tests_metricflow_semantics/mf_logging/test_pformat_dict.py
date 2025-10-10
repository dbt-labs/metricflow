from __future__ import annotations

import logging
import textwrap

from metricflow_semantics.toolkit.mf_logging.pretty_print import PrettyFormatDictOption, mf_pformat_dict
from metricflow_semantics.toolkit.string_helpers import mf_dedent

logger = logging.getLogger(__name__)


def test_pformat_many() -> None:  # noqa: D103
    result = mf_pformat_dict(
        "Example description:",
        obj_dict={"object_0": (1, 2, 3), "object_1": {4: 5}},
        format_option=PrettyFormatDictOption(max_line_length=30),
    )

    assert (
        textwrap.dedent(
            """\
            Example description:
              object_0: (1, 2, 3)
              object_1: {4: 5}
            """
        ).rstrip()
        == result
    )


def test_pformat_many_with_raw_strings() -> None:  # noqa: D103
    result = mf_pformat_dict(
        "Example description:",
        obj_dict={"object_0": "foo\nbar"},
        format_option=PrettyFormatDictOption(preserve_raw_strings=True, max_line_length=30),
    )

    assert (
        textwrap.dedent(
            """\
            Example description:
              object_0:
                foo
                bar
            """
        ).rstrip()
        == result
    )


def test_pformat_dict_with_empty_message() -> None:
    """Test `mf_pformat_dict` without a description."""
    result = mf_pformat_dict(
        obj_dict={"object_0": (1, 2, 3), "object_1": {4: 5}}, format_option=PrettyFormatDictOption(max_line_length=30)
    )

    assert (
        mf_dedent(
            """
            object_0: (1, 2, 3)
            object_1: {4: 5}
            """
        )
        == result
    )


def test_pformat_dict_with_pad_sections_with_newline() -> None:
    """Test `mf_pformat_dict` with new lines between sections."""
    result = mf_pformat_dict(
        obj_dict={"object_0": (1, 2, 3), "object_1": {4: 5}},
        format_option=PrettyFormatDictOption(pad_items_with_newlines=True, max_line_length=30),
    )

    assert (
        mf_dedent(
            """
            object_0: (1, 2, 3)

            object_1: {4: 5}
            """
        )
        == result
    )


def test_pformat_many_with_strings() -> None:  # noqa: D103
    result = mf_pformat_dict(
        "Example description:",
        obj_dict={"object_0": "foo\nbar"},
        format_option=PrettyFormatDictOption(max_line_length=30),
    )
    assert (
        textwrap.dedent(
            """\
            Example description:
              object_0: 'foo\\nbar'
            """
        ).rstrip()
        == result
    )


def test_minimal_length() -> None:
    """Test where the max_line_length is the minimal length."""
    assert mf_pformat_dict(
        "Example output", {"foo": "bar"}, format_option=PrettyFormatDictOption(max_line_length=1)
    ) == mf_dedent(
        """
        Example output
          foo: 'bar'
        """
    )


def test_one_line() -> None:
    """Test formatting as a one-line string if possible."""
    assert (
        mf_pformat_dict("Example output", {"a": 1, "b": 2}, format_option=PrettyFormatDictOption(max_line_length=80))
        == "Example output (a=1, b=2)"
    )
