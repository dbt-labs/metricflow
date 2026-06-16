from __future__ import annotations

import pprint
from collections.abc import Sequence, Set
from dataclasses import fields, is_dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Final, List, Mapping, Optional, Sized, Union

from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_logging.format_option import PrettyFormatOption
from metricflow_semantics.toolkit.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.toolkit.string_helpers import mf_indent


class MetricFlowPrettyFormatter:
    """Creates string representations of objects useful for logging / snapshots."""

    _DEFAULT_FORMAT_OPTION: Final[PrettyFormatOption] = PrettyFormatOption()

    def __init__(
        self,
        format_option: Optional[PrettyFormatOption] = None,
    ) -> None:
        """See mf_pformat() for argument descriptions."""
        self._format_option = (
            format_option if format_option is not None else MetricFlowPrettyFormatter._DEFAULT_FORMAT_OPTION
        )
        max_line_length = self._format_option.max_line_length
        if max_line_length is not None and max_line_length <= 0:
            raise ValueError(f"max_line_length must be > 0 as required by pprint.pformat(). Got {max_line_length}")

    def _handle_sequence_like_obj(
        self, sequence_like_obj: Union[Sequence, Set], remaining_line_length: Optional[int]
    ) -> str:
        """Pretty prints a sequence object i.e. list or tuple.

        Args:
            sequence_like_obj: A sequence-like object, including `Set`.
            remaining_line_length: If specified, try to make the string representation <= this many columns wide.

        Returns:
            A string representation of the sequence. e.g. `(1, 2), [1, 2], {1, 2}`
        """
        include_trailing_comma_for_single_item_in_one_line = False
        if isinstance(sequence_like_obj, tuple):
            left_enclose_str = "("
            right_enclose_str = ")"
            include_trailing_comma_for_single_item_in_one_line = True
        elif isinstance(sequence_like_obj, Sequence):
            left_enclose_str = "["
            right_enclose_str = "]"
        elif isinstance(sequence_like_obj, Set):
            left_enclose_str = "{"
            right_enclose_str = "}"
        else:
            raise RuntimeError(f"Unhandled type: {type(sequence_like_obj)}")

        if len(sequence_like_obj) == 0:
            return f"{left_enclose_str}{right_enclose_str}"

        # See if this object can be printed in one line.
        items_as_str = tuple(
            self._handle_any_obj(sequence_item, remaining_line_length=None) for sequence_item in sequence_like_obj
        )
        line_items = [left_enclose_str]
        if len(items_as_str) > 0:
            line_items.extend([", ".join(items_as_str)])
            if include_trailing_comma_for_single_item_in_one_line and len(items_as_str) == 1:
                line_items.append(",")
        line_items.append(right_enclose_str)
        result_without_width_limit = "".join(line_items)

        if remaining_line_length is None or len(result_without_width_limit) <= remaining_line_length:
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
                sequence_item,
                remaining_line_length=max(1, remaining_line_length - len(self._format_option.indent_prefix)),
            )
            for sequence_item in sequence_like_obj
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
            lines.append(mf_indent(item_block, indent_prefix=self._format_option.indent_prefix))
        lines.append(right_enclose_str)
        return "\n".join(lines)

    def _handle_indented_key_value_item(  # type: ignore[misc]
        self,
        key: Any,
        value: Any,
        key_value_seperator: str,
        is_dataclass_like_object: bool,
        remaining_line_length: Optional[int],
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
            remaining_line_length: If specified, try to make the string representation <= this many columns wide.

        Returns:
            The block that represents the key / value item and goes in between "[" / "]" in the string representation
            of the mapping-like object.
        """
        # See if the string representation can fit on one line. e.g. "'a': [1, 2]"
        result_items_without_limit: List[str] = []
        if is_dataclass_like_object and self._format_option.include_object_field_names:
            result_items_without_limit.append(str(key))
        else:
            result_items_without_limit.append(self._handle_any_obj(key, remaining_line_length=None))
        result_items_without_limit.append(key_value_seperator)
        result_items_without_limit.append(self._handle_any_obj(value, remaining_line_length=None))

        result_without_limit = mf_indent(
            "".join(result_items_without_limit), indent_prefix=self._format_option.indent_prefix
        )
        if remaining_line_length is None or len(result_without_limit) <= remaining_line_length:
            return result_without_limit

        # The string representation can't fit on one line - use multiple lines. e.g.
        """
        'key':
          [1, 2, 3, 4]
        """

        # Create the string for the key.
        result_lines: List[str] = []
        if is_dataclass_like_object and self._format_option.include_object_field_names:
            result_lines.append(str(key) + key_value_seperator)
        else:
            # See if the key can be printed on one line. This depends on the length of the value as the key and the
            # the value as at least the first bit of the value is printed on the same line as the key.
            # e.g.
            # "foo"=[
            #   ...
            # ]
            min_length_of_first_value_line = len(self._handle_any_obj(value, remaining_line_length=1).splitlines()[0])

            key_lines = self._handle_any_obj(
                key,
                remaining_line_length=max(
                    1,
                    remaining_line_length - len(key_value_seperator) - min_length_of_first_value_line,
                ),
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
        remaining_width_for_value = max(1, remaining_line_length - len(result_lines[-1]))
        value_str = self._handle_any_obj(value, remaining_line_length=remaining_width_for_value)
        value_lines = value_str.splitlines()

        if len(value_lines) <= 1:
            # Value can fit in the previous line
            result_lines[-1] = result_lines[-1] + value_lines[0]
        else:
            # For the multi-line value, we want to print the first line of the value on the same line as the last line
            # of the key.
            result_lines[-1] = result_lines[-1] + value_lines[0]
            result_lines.extend(value_lines[1:])

        return mf_indent("\n".join(result_lines), indent_prefix=self._format_option.indent_prefix)

    def pretty_format_object_by_parts(self, class_name: str, field_mapping: Mapping) -> str:
        """Return the string representation given class name and a mapping of the field names to field values."""
        return self._handle_mapping_like_obj(
            mapping=field_mapping,
            left_enclose_str=class_name + "(",
            key_value_seperator="=",
            right_enclose_str=")",
            is_dataclass_like_object=True,
            remaining_line_length=self._format_option.max_line_length,
        )

    def _handle_mapping_like_obj(
        self,
        mapping: Mapping,
        left_enclose_str: str,
        key_value_seperator: str,
        right_enclose_str: str,
        is_dataclass_like_object: bool,
        remaining_line_length: Optional[int],
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
            remaining_line_length: If specified, try to make the string representation <= this many columns wide.

        Returns:
            A string representation of the mapping. e.g. "{'a'=[1, 2]}" or "Foo(a=[1, 2])".
        """
        # Skip key / values depending on the pretty-print configuration.
        if is_dataclass_like_object:
            new_mapping = {}
            for key, value in mapping.items():
                if not self._format_option.include_none_object_fields and value is None:
                    continue
                if not self._format_option.include_empty_object_fields and isinstance(value, Sized) and len(value) == 0:
                    continue
                if not self._format_option.include_underscore_prefix_fields and key.startswith("_"):
                    continue
                new_mapping[key] = value
            mapping = new_mapping

        if len(mapping) == 0:
            return f"{left_enclose_str}{right_enclose_str}"

        # Handle case if the string representation fits on one line.
        if remaining_line_length is None or remaining_line_length > 0:
            comma_separated_items: List[str] = []
            for key, value in mapping.items():
                key_value_str_items: List[str] = []

                if is_dataclass_like_object:
                    if self._format_option.include_object_field_names:
                        key_value_str_items.append(str(key))
                        key_value_str_items.append(key_value_seperator)
                else:
                    key_value_str_items.append(self._handle_any_obj(key, remaining_line_length=None))
                    key_value_str_items.append(key_value_seperator)
                key_value_str_items.append(self._handle_any_obj(value, remaining_line_length=None))
                comma_separated_items.append("".join(key_value_str_items))
            result_without_limit = "".join((left_enclose_str, ", ".join(comma_separated_items), right_enclose_str))

            if remaining_line_length is None or len(result_without_limit) <= remaining_line_length:
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
                    remaining_line_length=(remaining_line_length - len(self._format_option.indent_prefix)),
                )
            )
        lines = [left_enclose_str, ",\n".join(mapping_items_as_str) + ",", right_enclose_str]
        return "\n".join(lines)

    def _handle_any_obj(self, obj: Any, remaining_line_length: Optional[int]) -> str:  # type: ignore
        """Convert any object into a pretty string-representation.

        This is called recursively as sequences and mappings have constituent objects of any type.

        Args:
            obj: The object to convert.
            remaining_line_length: If specified, try to make the string representation <= this many columns wide.

        Returns:
            A pretty string-representation of the object.
        """
        if isinstance(obj, Enum):
            return obj.name

        # Check for strings first as they are also sequences.
        if isinstance(obj, str):
            return self._handle_using_pprint(obj, remaining_line_length=remaining_line_length)

        if isinstance(obj, (Sequence, Set)):
            return self._handle_sequence_like_obj(obj, remaining_line_length=remaining_line_length)

        if isinstance(obj, dict):
            return self._handle_mapping_like_obj(
                obj,
                left_enclose_str="{",
                key_value_seperator=": ",
                right_enclose_str="}",
                is_dataclass_like_object=False,
                remaining_line_length=remaining_line_length,
            )

        if isinstance(obj, MetricFlowPrettyFormattable):
            result = obj.pretty_format(
                PrettyFormatContext(
                    formatter=MetricFlowPrettyFormatter(
                        format_option=self._format_option.with_max_line_length(remaining_line_length)
                    )
                )
            )
            if result is not None:
                return result

        if is_dataclass(obj):
            # dataclasses.asdict() seems to exclude None fields, so doing this instead.
            mapping = {field.name: getattr(obj, field.name) for field in fields(obj)}
            return self._handle_mapping_like_obj(
                mapping,
                left_enclose_str=type(obj).__name__ + "(",
                key_value_seperator="=",
                right_enclose_str=")",
                is_dataclass_like_object=True,
                remaining_line_length=remaining_line_length,
            )

        # For Pydantic-like objects with a `dict`-like method that returns field keys / values.
        # In Pydantic v1, it's `.dict()`, in v2 it's `.model_dump()`.
        # Going with this approach for now to check for a Pydantic model as using `isinstance()` requires more
        # consideration when dealing with Pydantic v1 and Pydantic v2 objects.
        pydantic_dict_method = getattr(obj, "model_dump", None) or getattr(obj, "dict", None)
        if pydantic_dict_method is not None and callable(pydantic_dict_method):
            try:
                # Calling `dict` on a Pydantic model does not recursively convert nested fields into dictionaries,
                # which is what we want. `.model_dump()` does the recursive conversion.
                mapping = dict(obj)
                return self._handle_mapping_like_obj(
                    mapping,
                    left_enclose_str=type(obj).__name__ + "(",
                    key_value_seperator="=",
                    right_enclose_str=")",
                    is_dataclass_like_object=True,
                    remaining_line_length=remaining_line_length,
                )
            except Exception:
                # Fall back to built-in pretty-print in case the dict method can't be called. e.g. requires arguments.
                # Consider logging a warning.
                pass

        if isinstance(obj, Path):
            return str(obj)

        # Any other object that's not handled.
        return self._handle_using_pprint(obj, remaining_line_length=remaining_line_length)

    def _handle_using_pprint(self, obj: Any, remaining_line_length: Optional[int]) -> str:  # type: ignore[misc]
        if remaining_line_length is not None:
            return pprint.pformat(obj, width=remaining_line_length, sort_dicts=False)
        else:
            return pprint.pformat(obj, sort_dicts=False)

    def pretty_format(self, obj: Any) -> str:  # type: ignore[misc]
        """Return a pretty string representation of the object that's suitable for logging."""
        return self._handle_any_obj(obj, remaining_line_length=self._format_option.max_line_length)

    @property
    def format_option(self) -> PrettyFormatOption:
        """Return the formatting option used to create this."""
        return self._format_option


@fast_frozen_dataclass()
class PrettyFormatContext:
    """The context to use for pretty-formatting an object."""

    formatter: MetricFlowPrettyFormatter
