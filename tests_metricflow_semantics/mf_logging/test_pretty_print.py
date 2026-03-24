from __future__ import annotations

import logging
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dbt_semantic_interfaces.implementations.elements.dimension import PydanticDimension
from dbt_semantic_interfaces.type_enums import DimensionType
from metricflow_semantics.test_helpers.metric_time_dimension import MTD_SPEC_DAY
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet
from metricflow_semantics.toolkit.mf_logging.format_option import PrettyFormatOption
from metricflow_semantics.toolkit.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.toolkit.mf_logging.pretty_formatter import (
    MetricFlowPrettyFormatter,
    PrettyFormatContext,
)
from metricflow_semantics.toolkit.mf_logging.pretty_print import PrettyFormatDictOption, mf_pformat
from metricflow_semantics.toolkit.string_helpers import mf_dedent, mf_indent
from typing_extensions import override

logger = logging.getLogger(__name__)


def test_literals() -> None:  # noqa: D103
    assert mf_pformat(1) == "1"
    assert mf_pformat(1.0) == "1.0"
    assert mf_pformat("foo") == "'foo'"


def test_containers() -> None:  # noqa: D103
    assert mf_pformat((1,)) == "(1,)"
    assert mf_pformat([1]) == "[1]"
    assert mf_pformat([1], format_option=PrettyFormatOption(max_line_length=1)) == mf_dedent(
        """
        [
          1,
        ]
        """
    )
    assert mf_pformat(((1, 2), 3)) == "((1, 2), 3)"
    assert mf_pformat([[1, 2], 3]) == "[[1, 2], 3]"
    assert mf_pformat({"a": ((1, 2), 3), (1, 2): 3}) == "{'a': ((1, 2), 3), (1, 2): 3}"
    assert mf_pformat({1, 2, 3}) == "{1, 2, 3}"
    assert mf_pformat(frozenset({1, 2, 3})) == "{1, 2, 3}"


def test_classes() -> None:  # noqa: D103
    assert "TimeDimensionSpec('metric_time', ExpandedTimeGranularity('day', DAY))" == mf_pformat(
        MTD_SPEC_DAY,
        format_option=PrettyFormatDictOption(
            include_object_field_names=False,
            include_none_object_fields=False,
            include_empty_object_fields=False,
        ),
    )

    assert (
        textwrap.dedent(
            """\
            TimeDimensionSpec(
              element_name='metric_time',
              entity_links=(),
              alias=None,
              time_granularity=ExpandedTimeGranularity(name='day', base_granularity=DAY),
              date_part=None,
              aggregation_state=None,
              window_functions=(),
            )
            """
        ).rstrip()
        == mf_pformat(
            MTD_SPEC_DAY,
            format_option=PrettyFormatDictOption(
                include_object_field_names=True,
                include_none_object_fields=True,
                include_empty_object_fields=True,
            ),
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
        result = mf_pformat(
            obj={(1,): (4, 5, 6)}, format_option=PrettyFormatDictOption(max_line_length=max_line_length)
        )
        if result != previous_result:
            output_lines.append(f"max_line_length={max_line_length}:")
            output_lines.append(mf_indent(result))
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
        result = mf_pformat(obj={(1,): 2}, format_option=PrettyFormatDictOption(max_line_length=max_line_length))
        if result != previous_result:
            output_lines.append(f"max_line_length={max_line_length}:")
            output_lines.append(mf_indent(result))
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


def test_custom_pretty_print() -> None:
    """Check that `MetricFlowPrettyFormattable` can be used to override the result when using MF's pretty-printer."""

    @dataclass(frozen=True)
    class _ExampleDataclass(MetricFlowPrettyFormattable):
        field_0: float

        @override
        def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
            """Print this like a dictionary instead field as a string to 2 decimal places."""
            return format_context.formatter.pretty_format({"field_0": f"{self.field_0:.2f}"})

    assert mf_pformat(_ExampleDataclass(1.2345)) == "{'field_0': '1.23'}"


def test_include_underscore_prefix_option() -> None:  # noqa: D103
    @dataclass(frozen=True)
    class _ExampleDataclass:
        field_0: int
        _hidden_field: int

    assert (
        mf_pformat(
            _ExampleDataclass(field_0=1, _hidden_field=2),
            format_option=PrettyFormatOption(include_underscore_prefix_fields=False),
        )
        == "_ExampleDataclass(field_0=1)"
    )
    assert (
        mf_pformat(
            _ExampleDataclass(field_0=1, _hidden_field=2),
            format_option=PrettyFormatOption(include_underscore_prefix_fields=True),
        )
        == "_ExampleDataclass(field_0=1, _hidden_field=2)"
    )


def test_format_object_by_parts() -> None:  # noqa: D103
    formatter = MetricFlowPrettyFormatter(PrettyFormatOption())
    assert "_ExampleDataclass(field_0=1)" == formatter.pretty_format_object_by_parts(
        class_name="_ExampleDataclass", field_mapping={"field_0": 1}
    )


def test_path() -> None:
    """Test formatting of `Path` objects.

    It should be represented as `/a/b/c` instead of  `PosixPath("/a/b/c")`.
    """
    assert mf_pformat(Path("/a/b/c")) == "/a/b/c"


def test_ordered_set() -> None:  # noqa: D103
    ordered_set = FrozenOrderedSet((3, 2, 1))
    assert mf_pformat(ordered_set) == "{3, 2, 1}"
