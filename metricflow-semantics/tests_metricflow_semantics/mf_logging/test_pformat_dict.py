from __future__ import annotations

import textwrap

from metricflow_semantics.formatting.formatting_helpers import mf_dedent
from metricflow_semantics.mf_logging.pretty_print import mf_pformat_dict


def test_pformat_many() -> None:  # noqa: D103
    result = mf_pformat_dict("Example description:", obj_dict={"object_0": (1, 2, 3), "object_1": {4: 5}})

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
    result = mf_pformat_dict("Example description:", obj_dict={"object_0": "foo\nbar"}, preserve_raw_strings=True)

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
    result = mf_pformat_dict(obj_dict={"object_0": (1, 2, 3), "object_1": {4: 5}})

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
    result = mf_pformat_dict(obj_dict={"object_0": (1, 2, 3), "object_1": {4: 5}}, pad_items_with_newlines=True)

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
    result = mf_pformat_dict("Example description:", obj_dict={"object_0": "foo\nbar"})
    assert (
        textwrap.dedent(
            """\
            Example description:
              object_0: 'foo\\nbar'
            """
        ).rstrip()
        == result
    )
