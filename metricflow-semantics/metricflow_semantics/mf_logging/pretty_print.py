from __future__ import annotations

import logging
import pprint
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from enum import Enum
from typing import Any, List, Optional, Sized, Tuple, Union

from dsi_pydantic_shim import BaseModel

from metricflow_semantics.mf_logging.formatting import indent
from metricflow_semantics.mf_logging.pretty_formattable import MetricFlowPrettyFormattable

logger = logging.getLogger(__name__)


class MetricFlowPrettyFormatter:
    """Creates string representations of objects useful for logging / snapshots."""

    def __init__(
        self,
        indent_prefix: str,
        max_line_length: int,
        include_object_field_names: bool,
        include_none_object_fields: bool,
        include_empty_object_fields: bool,
    ) -> None:
        """See mf_pformat() for argument descriptions."""
        self._indent_prefix = indent_prefix
        if not max_line_length > 0:
            raise ValueError(f"max_line_length must be > 0 as required by pprint.pformat(). Got {max_line_length}")
        self._max_line_width = max_line_length
        self._include_object_field_names = include_object_field_names
        self._include_none_object_fields = include_none_object_fields
        self._include_empty_object_fields = include_empty_object_fields

    @staticmethod
    def _is_pydantic_base_model(obj: Any):  # type:ignore
        # Checking the attribute as the BaseModel check fails for newer version of Pydantic.
        return isinstance(obj, BaseModel) or hasattr(obj, "__config__")

    def _handle_sequence_obj(
        self, list_like_obj: Union[list, tuple, set, frozenset], remaining_line_width: Optional[int]
    ) -> str:
        """Pretty prints a sequence object i.e. list or tuple.

        Args:
            list_like_obj: A list, tuple, set, or frozenset.
            remaining_line_width: If specified, try to make the string representation <= this many columns wide.

        Returns:
            A string representation of the sequence like (1,) or [1, 2].
        """
        if isinstance(list_like_obj, list):
            left_enclose_str = "["
            right_enclose_str = "]"
        elif isinstance(list_like_obj, tuple):
            left_enclose_str = "("
            right_enclose_str = ")"
        elif isinstance(list_like_obj, set) or isinstance(list_like_obj, frozenset):
            left_enclose_str = f"{type(list_like_obj).__name__}("
            right_enclose_str = ")"
            list_like_obj = sorted(self._handle_any_obj(obj, None) for obj in list_like_obj)
        else:
            raise RuntimeError(f"Unhandled type: {type(list_like_obj)}")

        if len(list_like_obj) == 0:
            return f"{left_enclose_str}{right_enclose_str}"

        # See if this object can be printed in one line.
        items_as_str = tuple(self._handle_any_obj(list_item, remaining_line_width=None) for list_item in list_like_obj)
        line_items = [left_enclose_str]
        if len(items_as_str) > 0:
            line_items.extend([", ".join(items_as_str)])
            if len(items_as_str) == 1:
                line_items.append(",")
        line_items.append(right_enclose_str)
        result_without_width_limit = "".join(line_items)

        if remaining_line_width is None or len(result_without_width_limit) <= remaining_line_width:
            return result_without_width_limit

        # The item can't be printed on one line, so do an indented style like:
        """
        [
            1,
            2,
            ...
        ]
        """

        # Convert each item to a pretty string.
        items_as_str = tuple(
            self._handle_any_obj(
                list_item, remaining_line_width=max(0, remaining_line_width - len(self._indent_prefix))
            )
            for list_item in list_like_obj
        )
        lines = [left_enclose_str]

        # item_block is similar to
        """
        1,
        2,
        3,
        """

        item_block = ",\n".join(items_as_str) + ","
        # Indent the item_block
        if len(item_block) > 0:
            lines.append(indent(item_block, indent_prefix=self._indent_prefix))
        lines.append(right_enclose_str)
        return "\n".join(lines)

    def _handle_indented_key_value_item(  # type: ignore[misc]
        self,
        key: Any,
        value: Any,
        key_value_seperator: str,
        is_dataclass_like_object: bool,
        remaining_line_width: Optional[int],
    ) -> str:
        """Convert a key / value for a mapping-like object to a string that should be placed in an indented block.

        Mapping-like objects include dictionaries, dataclasses, and Pydantic models. The output of this method would
        look like:

        "'key': [1, 2, 3]" or "arg=Foo()", etc.

        and the caller of this method would add the actual indent.

        Args:
            key: The object representing the key.
            value: The object representing the value.
            key_value_seperator: The string used to separate the key and the value. e.g. ": " for dicts, "=" for
            dataclasses.
            is_dataclass_like_object: Set this to True if the given value object is a dataclass to handle some printing
            options specific to dataclasses.
            remaining_line_width: If specified, try to make the string representation <= this many columns wide.

        Returns:
            The block that represents the key / value item and goes in between "[" / "]" in the string representation
            of the mapping-like object.
        """
        # See if the string representation can fit on one line. e.g. "'a': [1, 2]"
        if remaining_line_width is None or remaining_line_width > 0:
            result_items_without_limit: List[str] = []
            if is_dataclass_like_object and self._include_object_field_names:
                result_items_without_limit.append(str(key))
            else:
                result_items_without_limit.append(self._handle_any_obj(key, remaining_line_width=None))
            result_items_without_limit.append(key_value_seperator)
            result_items_without_limit.append(self._handle_any_obj(value, remaining_line_width=None))

            result_without_limit = indent("".join(result_items_without_limit), indent_prefix=self._indent_prefix)
            if remaining_line_width is None or len(result_without_limit) <= remaining_line_width:
                return result_without_limit

        # The string representation can't fit on one line - use multiple. e.g.
        """
        'key':
          [1, 2, 3, 4]
        """

        # Create the string for the key.
        result_lines: List[str] = []
        if is_dataclass_like_object and self._include_object_field_names:
            result_lines.append(str(key) + key_value_seperator)
        else:
            # See if the key can be printed on one line. This depends on the length of the value as the key and the
            # the value as at least the first bit of the value is printed on the same line as the key.
            # e.g.
            # "foo"=[
            #   ...
            # ]
            min_length_of_first_value_line = len(self._handle_any_obj(value, remaining_line_width=0).splitlines()[0])

            key_lines = self._handle_any_obj(
                key,
                remaining_line_width=remaining_line_width - len(key_value_seperator) - min_length_of_first_value_line,
            ).splitlines()
            # key_lines would be something like:
            # [
            #     "KeyObject(",
            #     "    a='foo',",
            #     "    b='bar',",
            #     ")",
            # ]

            if len(key_lines) == 1:
                result_lines.append(key_lines[0] + key_value_seperator)
            else:
                # The key needs to be printed in multiple lines. In that case, we want a result where the key value
                # separator is on the last line with the key. e.g.
                #
                # KeyObject(
                #     a='foo',
                #     b='bar',
                # ): ... <value>
                #
                result_lines.extend(key_lines[:-1])
                result_lines.append(key_lines[-1] + key_value_seperator)

        # Combine key and value.

        # Similar to the key, how we print the value depends on whether the value fits on one line or not. e.g.
        # foo=[1, 2, 3]
        #
        # or
        #
        # foo=[
        #   1,
        #   2,
        #   3,
        # ]

        # See if the value fits in the previous line.
        remaining_width_for_value = max(0, remaining_line_width - len(result_lines[-1]))
        value_str = self._handle_any_obj(value, remaining_line_width=remaining_width_for_value)
        value_lines = value_str.splitlines()

        if len(value_lines) <= 1:
            # Value can fit in the previous line
            result_lines[-1] = result_lines[-1] + value_lines[0]
        else:
            # For the multi-line value, we want to print the first line of the value on the same line as the last line
            # of the key.
            result_lines[-1] = result_lines[-1] + value_lines[0]
            result_lines.extend(value_lines[1:])

        return indent("\n".join(result_lines), indent_prefix=self._indent_prefix)

    def _handle_mapping_like_obj(
        self,
        mapping: Mapping,
        left_enclose_str: str,
        key_value_seperator: str,
        right_enclose_str: str,
        is_dataclass_like_object: bool,
        remaining_line_width: Optional[int],
    ) -> str:
        """Convert a mapping-like object to a pretty string.

        This class treats dataclasses as mappings where the field / field values are the keys / values.


        Args:
            mapping: The mapping object to convert.
            left_enclose_str: The string used on the left side to enclose the object. e.g. "{" for dicts or "Foo(" for
            dataclasses.
            key_value_seperator: The string used to separate keys and values. e.g. ": " for dicts, or "=" for
            dataclasses.
            right_enclose_str: The string used on the right side to enclose the object. e.g. "}" for dicts or ")" for
            dataclasses.
            is_dataclass_like_object: Flag to indicate whether this is a dataclass as there are some differences in
            formatting those.
            remaining_line_width: If specified, try to make the string representation <= this many columns wide.

        Returns:
            A string representation of the mapping. e.g. "{'a'=[1, 2]}" or "Foo(a=[1, 2])".
        """
        # Skip key / values depending on the pretty-print configuration.
        if is_dataclass_like_object and not self._include_none_object_fields:
            mapping = {key: value for key, value in mapping.items() if value is not None}

        if is_dataclass_like_object and not self._include_empty_object_fields:
            mapping = {
                key: value
                for key, value in mapping.items()
                if (isinstance(value, Sized) and len(value) > 0) or (not isinstance(value, Sized))
            }

        if len(mapping) == 0:
            return f"{left_enclose_str}{right_enclose_str}"

        # Handle case if the string representation fits on one line.
        if remaining_line_width is None or remaining_line_width > 0:
            comma_separated_items: List[str] = []
            for key, value in mapping.items():
                key_value_str_items: List[str] = []

                if is_dataclass_like_object:
                    if self._include_object_field_names:
                        key_value_str_items.append(str(key))
                        key_value_str_items.append(key_value_seperator)
                else:
                    key_value_str_items.append(self._handle_any_obj(key, remaining_line_width=None))
                    key_value_str_items.append(key_value_seperator)
                key_value_str_items.append(self._handle_any_obj(value, remaining_line_width=None))
                comma_separated_items.append("".join(key_value_str_items))
            result_without_limit = "".join((left_enclose_str, ", ".join(comma_separated_items), right_enclose_str))

            if remaining_line_width is None or len(result_without_limit) <= remaining_line_width:
                return result_without_limit

        # Handle multi-line case.
        mapping_items_as_str = []
        for key, value in mapping.items():
            mapping_items_as_str.append(
                self._handle_indented_key_value_item(
                    key=key,
                    value=value,
                    key_value_seperator=key_value_seperator,
                    is_dataclass_like_object=is_dataclass_like_object,
                    remaining_line_width=(remaining_line_width - len(self._indent_prefix)),
                )
            )
        lines = [left_enclose_str, ",\n".join(mapping_items_as_str) + ",", right_enclose_str]
        return "\n".join(lines)

    def _handle_any_obj(self, obj: Any, remaining_line_width: Optional[int]) -> str:  # type: ignore
        """Convert any object into a pretty string-representation.

        This is called recursively as sequences and mappings have constituent objects of any type.

        Args:
            obj: The object to convert.
            remaining_line_width: If specified, try to make the string representation <= this many columns wide.

        Returns:
            A pretty string-representation of the object.
        """
        if isinstance(obj, Enum):
            return obj.name

        if isinstance(obj, (list, tuple, set, frozenset)):
            return self._handle_sequence_obj(obj, remaining_line_width=remaining_line_width)

        if isinstance(obj, dict):
            return self._handle_mapping_like_obj(
                obj,
                left_enclose_str="{",
                key_value_seperator=": ",
                right_enclose_str="}",
                is_dataclass_like_object=False,
                remaining_line_width=remaining_line_width,
            )

        if isinstance(obj, MetricFlowPrettyFormattable):
            if obj.pretty_format is not None:
                return obj.pretty_format

        if is_dataclass(obj):
            # dataclasses.asdict() seems to exclude None fields, so doing this instead.
            mapping = {field.name: getattr(obj, field.name) for field in fields(obj)}
            return self._handle_mapping_like_obj(
                mapping,
                left_enclose_str=type(obj).__name__ + "(",
                key_value_seperator="=",
                right_enclose_str=")",
                is_dataclass_like_object=True,
                remaining_line_width=remaining_line_width,
            )

        if MetricFlowPrettyFormatter._is_pydantic_base_model(obj):
            mapping = {key: getattr(obj, key) for key in obj.dict().keys()}
            return self._handle_mapping_like_obj(
                mapping,
                left_enclose_str=type(obj).__name__ + "(",
                key_value_seperator="=",
                right_enclose_str=")",
                is_dataclass_like_object=True,
                remaining_line_width=remaining_line_width,
            )

        # Any other object that's not handled.
        return pprint.pformat(obj, width=self._max_line_width, sort_dicts=False)

    def pretty_format(self, obj: Any) -> str:  # type: ignore[misc]
        """Return a pretty string representation of the object that's suitable for logging."""
        return self._handle_any_obj(obj, remaining_line_width=self._max_line_width)


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
    for key, value in obj_dict.items():
        if preserve_raw_strings and isinstance(value, str):
            value_str = value
        else:
            value_str = mf_pformat(
                obj=value,
                max_line_length=max(0, max_line_length - len(indent_prefix)),
                indent_prefix=indent_prefix,
                include_object_field_names=include_object_field_names,
                include_none_object_fields=include_none_object_fields,
                include_empty_object_fields=include_empty_object_fields,
            )

        lines_in_value_str = len(value_str.split("\n"))
        item_section_lines: Tuple[str, ...]
        if lines_in_value_str > 1:
            item_section_lines = (
                f"{key}:",
                indent(
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
            item_sections.append(indent(item_section))

    if pad_items_with_newlines:
        return "\n\n".join(description_lines + item_sections)
    else:
        return "\n".join(description_lines + item_sections)
