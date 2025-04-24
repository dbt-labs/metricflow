from __future__ import annotations

import logging
from typing import Any, Dict, List, Mapping, Optional, Tuple

from metricflow_semantics.helpers.string_helpers import mf_indent
from metricflow_semantics.mf_logging.pretty_formatter import MetricFlowPrettyFormatter

logger = logging.getLogger(__name__)


def mf_pformat(  # type: ignore
    obj: Any,
    max_line_length: int = 120,
    indent_prefix: str = "  ",
    include_object_field_names: bool = True,
    include_none_object_fields: bool = False,
    include_empty_object_fields: bool = False,
) -> str:
    """Print objects in a pretty way for logging / test snapshots.

    In Python 3.10, the pretty printer class will support dataclasses, so we can remove this once we're on
    3.10. Also tried the prettyprint package with dataclasses, but that prints full names for the classes
    e.g. a.b.MyClass and it also always added line breaks, even if an object could fit on one line, so
    preferring to not use that for compactness.

    e.g.
        metricflow.specs.DimensionSpec(
            element_name='country',
            entity_links=()
        ),

    Instead, the below will print something like:

        DimensionSpec(element_name='country', entity_links=())

    Also, this simplifies the object representation in some cases (e.g. Enums) and provides options for a more compact
    string. This is an improvement on pformat_big_objects() in dbt-semantic-interfaces to be more compact and easier
    to read.

    Args:
        obj: The object to convert to string.
        max_line_length: If the string representation is going to be longer than this, split into multiple lines.
        indent_prefix: The prefix to use for hierarchical indents.
        include_object_field_names: Include field names when printing objects - e.g. Foo(bar='baz') vs Foo('baz')
        include_none_object_fields: Include fields with a None value - e.g. Foo(bar=None) vs Foo()
        include_empty_object_fields: Include fields that are empty - e.g. Foo(bar=()) vs Foo()

    Returns:
        A string representation of the object that's useful for logging / debugging.
    """
    # Since this is used in logging calls, wrap with except so that a bug here doesn't result in something breaking.
    try:
        formatter = MetricFlowPrettyFormatter(
            indent_prefix=indent_prefix,
            max_line_length=max_line_length,
            include_object_field_names=include_object_field_names,
            include_none_object_fields=include_none_object_fields,
            include_empty_object_fields=include_empty_object_fields,
        )
        return formatter.pretty_format(obj)
    except Exception:
        # This automatically includes the call trace.
        logger.exception("Error pretty printing due to an exception - using str() instead.")
        return str(obj)


def mf_pformat_dict(  # type: ignore
    description: Optional[str] = None,
    obj_dict: Optional[Mapping[str, Any]] = None,
    max_line_length: int = 120,
    indent_prefix: str = "  ",
    include_object_field_names: bool = True,
    include_none_object_fields: bool = False,
    include_empty_object_fields: bool = False,
    preserve_raw_strings: bool = False,
    pad_items_with_newlines: bool = False,
) -> str:
    """Prints many objects in an indented form.

    If `preserve_raw_strings` is set, and a value of the obj_dict is of type str, then use the value itself, not the
    representation of the string. e.g. if value="foo", then "foo" instead of "'foo'". Useful for values that contain
    newlines.

    If `pad_items_with_newlines` is set , each key / value section is padded with newlines.
    """
    description_lines: List[str] = [description] if description is not None else []
    obj_dict = obj_dict or {}
    item_sections = []

    str_converted_dict: Dict[str, str] = {}
    for key, value in obj_dict.items():
        if preserve_raw_strings and isinstance(value, str):
            value_str = value
        else:
            value_str = mf_pformat(
                obj=value,
                max_line_length=max(1, max_line_length - len(indent_prefix)),
                indent_prefix=indent_prefix,
                include_object_field_names=include_object_field_names,
                include_none_object_fields=include_none_object_fields,
                include_empty_object_fields=include_empty_object_fields,
            )
        str_converted_dict[str(key)] = value_str

        item_section_lines: Tuple[str, ...]
        if "\n" in value_str:
            item_section_lines = (
                f"{key}:",
                mf_indent(
                    value_str,
                    indent_prefix=indent_prefix,
                ),
            )
        else:
            item_section_lines = (f"{key}: {value_str}",)
        item_section = "\n".join(item_section_lines)

        if description is None:
            item_sections.append(item_section)
        else:
            item_sections.append(mf_indent(item_section))

    result_as_one_line = _as_one_line(
        description=description, str_converted_dict=str_converted_dict, max_line_length=max_line_length
    )
    if result_as_one_line is not None:
        return result_as_one_line

    if pad_items_with_newlines:
        return "\n\n".join(description_lines + item_sections)
    else:
        return "\n".join(description_lines + item_sections)


def _as_one_line(description: Optional[str], str_converted_dict: Dict[str, str], max_line_length: int) -> Optional[str]:
    """See if the result can be returned in a compact, one-line representation.

    e.g. for:
      mf_pformat_dict("Example output", {"a": 1, "b": 2})

    Compact output:
      Example output (a=1, b=2)

    Normal output:
      Example output
        a: 1
        b: 2
    """
    items = tuple(f"{key_str}={value_str}" for key_str, value_str in str_converted_dict.items())
    value_in_parenthesis = ", ".join(items)
    result = f"{description}" + (f" ({value_in_parenthesis})" if len(value_in_parenthesis) > 0 else "")

    if "\n" in result or len(result) > max_line_length:
        return None

    return result
