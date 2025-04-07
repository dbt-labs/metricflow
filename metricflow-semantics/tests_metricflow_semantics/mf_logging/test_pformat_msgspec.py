from __future__ import annotations

import logging
from typing import Optional

import msgspec
from metricflow_semantics.formatting.formatting_helpers import mf_dedent
from metricflow_semantics.mf_logging.pretty_print import mf_pformat

logger = logging.getLogger(__name__)


class _ExampleStruct(msgspec.Struct):
    int_field: int
    string_field: str
    recursive_field: Optional[_ExampleStruct]


def test_struct() -> None:
    """Test formatting of a `msgspec.Struct`."""
    inner_struct = _ExampleStruct(int_field=1, string_field="inner_struct__string_field", recursive_field=None)
    outer_struct = _ExampleStruct(int_field=0, string_field="outer_struct__string_field", recursive_field=inner_struct)
    result = mf_pformat(outer_struct, max_line_length=1)
    assert result == mf_dedent(
        """
        _ExampleStruct(
          int_field=0,
          string_field='outer_struct__string_field',
          recursive_field=_ExampleStruct(
            int_field=1,
            string_field='inner_struct__string_field',
          ),
        )
        """
    )
