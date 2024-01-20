from __future__ import annotations

import logging
import textwrap

from dbt_semantic_interfaces.implementations.elements.dimension import PydanticDimension
from dbt_semantic_interfaces.type_enums import DimensionType

from metricflow.mf_logging.formatting import indent
from metricflow.mf_logging.pretty_print import mf_pformat, mf_pformat_many
from metricflow.test.time.metric_time_dimension import MTD_SPEC_DAY

logger = logging.getLogger(__name__)


def test_literals() -> None:  # noqa: D
    assert mf_pformat(1) == "1"
    assert mf_pformat(1.0) == "1.0"
    assert mf_pformat("foo") == "'foo'"


def test_containers() -> None:  # noqa: D
    assert mf_pformat((1,)) == "(1,)"
    assert mf_pformat(((1, 2), 3)) == "((1, 2), 3)"
    assert mf_pformat([[1, 2], 3]) == "[[1, 2], 3]"
    assert mf_pformat({"a": ((1, 2), 3), (1, 2): 3}) == "{'a': ((1, 2), 3), (1, 2): 3}"


def test_classes() -> None:  # noqa: D
    assert "TimeDimensionSpec('metric_time', DAY)" == mf_pformat(
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
              time_granularity=DAY,
              date_part=None,
              aggregation_state=None,
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

    assert "TimeDimensionSpec(element_name='metric_time', time_granularity=DAY)" == mf_pformat(MTD_SPEC_DAY)


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


def test_pydantic_model() -> None:  # noqa: D
    assert "PydanticDimension(name='foo', type=CATEGORICAL, is_partition=False)" == mf_pformat(
        PydanticDimension(name="foo", type=DimensionType.CATEGORICAL)
    )


def test_pformat_many() -> None:  # noqa: D
    result = mf_pformat_many("Example description:", obj_dict={"object_0": (1, 2, 3), "object_1": {4: 5}})

    assert (
        textwrap.dedent(
            """\
            Example description:

            object_0:
              (1, 2, 3)

            object_1:
              {4: 5}
            """
        ).rstrip()
        == result
    )
