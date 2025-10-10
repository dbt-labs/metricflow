from __future__ import annotations

import logging
from typing import Optional

from typing_extensions import Self, override

from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.merger import Mergeable
from metricflow_semantics.toolkit.syntactic_sugar import mf_first_non_none, mf_first_non_none_or_raise

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class PrettyFormatOption(Mergeable):
    """Options for `mf_pformat`.

    max_line_length: If the string representation is going to be longer than this, split into multiple lines.
    indent_prefix: The prefix to use for hierarchical indents.
    include_object_field_names: Include field names when printing objects - e.g. Foo(bar='baz') vs Foo('baz')
    include_none_object_fields: Include fields with a None value - e.g. Foo(bar=None) vs Foo()
    include_empty_object_fields: Include fields that are empty - e.g. Foo(bar=()) vs Foo()
    """

    max_line_length: Optional[int] = 120
    indent_prefix: str = "  "
    include_object_field_names: bool = True
    include_none_object_fields: bool = False
    include_empty_object_fields: bool = False
    include_underscore_prefix_fields: bool = False

    def with_max_line_length(self, max_line_length: Optional[int]) -> PrettyFormatOption:
        """Return a copy of self but with a new value for `max_line_length`."""
        return PrettyFormatOption(
            max_line_length=max_line_length,
            indent_prefix=self.indent_prefix,
            include_object_field_names=self.include_object_field_names,
            include_none_object_fields=self.include_none_object_fields,
            include_empty_object_fields=self.include_empty_object_fields,
        )

    @override
    def merge(self: Self, other: PrettyFormatOption) -> PrettyFormatOption:
        return PrettyFormatOption(
            max_line_length=mf_first_non_none(other.max_line_length, self.max_line_length),
            indent_prefix=mf_first_non_none_or_raise(other.indent_prefix, self.indent_prefix),
            include_object_field_names=mf_first_non_none_or_raise(
                other.include_object_field_names,
                self.include_object_field_names,
            ),
            include_none_object_fields=mf_first_non_none_or_raise(
                other.include_none_object_fields,
                self.include_none_object_fields,
            ),
            include_empty_object_fields=mf_first_non_none_or_raise(
                other.include_empty_object_fields,
                self.include_empty_object_fields,
            ),
            include_underscore_prefix_fields=mf_first_non_none_or_raise(
                other.include_underscore_prefix_fields,
                self.include_underscore_prefix_fields,
            ),
        )

    @classmethod
    @override
    def empty_instance(cls) -> PrettyFormatOption:
        return cls()
