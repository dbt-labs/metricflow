from __future__ import annotations

import logging
import textwrap
from dataclasses import dataclass
from typing import Optional

from dbt_semantic_interfaces.implementations.elements.dimension import PydanticDimension
from dbt_semantic_interfaces.type_enums import DimensionType
from metricflow_semantics.formatting.formatting_helpers import mf_dedent
from metricflow_semantics.mf_logging.formatting import indent
from metricflow_semantics.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.mf_logging.pretty_print import mf_pformat, mf_pformat_dict
from metricflow_semantics.test_helpers.metric_time_dimension import MTD_SPEC_DAY
from typing_extensions import override

logger = logging.getLogger(__name__)


def test_literals() -> None:  # noqa: D103
    assert mf_pformat(1) == "1"
    assert mf_pformat(1.0) == "1.0"
    assert mf_pformat("foo") == "'foo'"


def test_containers() -> None:  # noqa: D103
    assert mf_pformat((1,)) == "(1,)"
    assert mf_pformat(((1, 2), 3)) == "((1, 2), 3)"
    assert mf_pformat([[1, 2], 3]) == "[[1, 2], 3]"
    assert mf_pformat({"a": ((1, 2), 3), (1, 2): 3}) == "{'a': ((1, 2), 3), (1, 2): 3}"


def test_classes() -> None:  # noqa: D103
    assert "TimeDimensionSpec('metric_time', ExpandedTimeGranularity('day', DAY))" == mf_pformat(
        MTD_SPEC_DAY,
        include_object_field_names=False,
        include_none_object_fields=False,
        include_empty_object_fields=False,
    )

    assert (
        textwrap.dedent(
            """\
            TimeDimensionSpec(
              element_name='metric_time',
              entity_links=(),
              time_granularity=ExpandedTimeGranularity(name='day', base_granularity=DAY),
              date_part=None,
              aggregation_state=None,
              window_function=None,
            )
            """
        ).rstrip()
        == mf_pformat(
            MTD_SPEC_DAY,
            include_object_field_names=True,
            include_none_object_fields=True,
            include_empty_object_fields=True,
        )
    )

    assert (
        textwrap.dedent(
            """\
            TimeDimensionSpec(
              element_name='metric_time',
              time_granularity=ExpandedTimeGranularity(name='day', base_granularity=DAY),
            )
            """
        ).rstrip()
        == mf_pformat(MTD_SPEC_DAY)
    )


def test_multi_line_key_value_dict() -> None:
    """Test a dict where the key and value needs to be printed on multiple lines."""
    output_lines = []
    previous_result = None
    for max_line_length in range(1, 18):
        result = mf_pformat(obj={(1,): (4, 5, 6)}, max_line_length=max_line_length)
        if result != previous_result:
            output_lines.append(f"max_line_length={max_line_length}:")
            output_lines.append(indent(result))
            previous_result = result
    result = "\n".join(output_lines)
    assert (
        textwrap.dedent(
            """\
            max_line_length=1:
              {
                (
                  1,
                ): (
                  4,
                  5,
                  6,
                ),
              }
            max_line_length=9:
              {
                (1,): (
                  4,
                  5,
                  6,
                ),
              }
            max_line_length=17:
              {(1,): (4, 5, 6)}
            """
        ).rstrip()
        == result
    )


def test_multi_line_key_value_dict_short_value() -> None:
    """Similar to test_multi_line_key_value_dict but with a short value."""
    output_lines = []
    previous_result = None
    for max_line_length in range(1, 18):
        result = mf_pformat(obj={(1,): 2}, max_line_length=max_line_length)
        if result != previous_result:
            output_lines.append(f"max_line_length={max_line_length}:")
            output_lines.append(indent(result))
            previous_result = result
    result = "\n".join(output_lines)
    assert (
        textwrap.dedent(
            """\
            max_line_length=1:
              {
                (
                  1,
                ): 2,
              }
            max_line_length=9:
              {(1,): 2}
            """
        ).rstrip()
        == result
    )


def test_pydantic_model() -> None:  # noqa: D103
    assert "PydanticDimension(name='foo', type=CATEGORICAL, is_partition=False)" == mf_pformat(
        PydanticDimension(name="foo", type=DimensionType.CATEGORICAL)
    )


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


def test_custom_pretty_print() -> None:
    """Check that `MetricFlowPrettyFormattable` can be used to override the result when using MF's pretty-printer."""

    @dataclass(frozen=True)
    class _ExampleDataclass(MetricFlowPrettyFormattable):
        field_0: float

        @property
        @override
        def pretty_format(self) -> Optional[str]:
            """Only show 2 decimal points when pretty printing."""
            return f"{self.__class__.__name__}({self.field_0:.2f})"

    assert mf_pformat(_ExampleDataclass(1.2345)) == f"{_ExampleDataclass.__name__}(1.23)"


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
