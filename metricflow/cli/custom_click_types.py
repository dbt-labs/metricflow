"""This module contains custom helper click types."""

from __future__ import annotations

from typing import Any, Callable, Dict, Generic, List, Optional, Sequence, Tuple, TypeVar

import click

T = TypeVar("T")


# NOTE: unfortunately python 3.9 has no way of properly typing args and kwargs since it has no
# typing.ParamSpec. Since click uses args and kwargs, we have to add type ignore.
#
# Also, some click interfaces require us to return explicit Any.


class SequenceParamType(click.ParamType, Generic[T]):
    """A click parameter that is a list of elements.

    It can be used to parse strings like "1,2,4,8" into a Sequence[int], for example.
    """

    name = "sequence"

    def __init__(  # type: ignore
        self,
        value_converter: Callable[[str], T] = lambda x: x,  # type: ignore
        min_length: int = 0,
        max_length: Optional[int] = None,
        separator: str = ",",
        *args,
        **kwargs,
    ) -> None:
        """Initialize the sequence param type.

        value_converter: a function to convert each list value.
            Defaults to not converting and keeping them as strings.
        min_length: minimum length for the list, inclusive.
            Defaults to 0.
        max_length: maximum length for the list, inclusive.
            If None, there is no maximum length.
            Defaults to None.
        separator: the list separator.
            Defaults to `,`.
        """
        self.value_converter = value_converter
        self.min_length = min_length
        self.max_length = max_length
        self.separator = separator

        super().__init__(*args, **kwargs)

    def convert(  # noqa: D102
        self, value: str, param: Optional[click.Parameter], ctx: Optional[click.Context]
    ) -> Sequence[T]:  # noqa: D102
        if len(value) == 0:
            if self.min_length > 0:
                self.fail(
                    f"Input is empty, when the minimum expected number of input is {self.min_length}",
                    param,
                    ctx,
                )
            return []
        str_values = value.split(self.separator)

        if len(str_values) < self.min_length:
            self.fail(
                f"list has length {len(str_values)}, which is less than the minimum allowed length of {self.min_length}",
                param,
                ctx,
            )

        if self.max_length is not None and len(str_values) > self.max_length:
            self.fail(
                f"list has length {len(str_values)}, which is more than the maximum allowed length of {self.max_length}",
                param,
                ctx,
            )

        converted_values: List[T] = []
        for str_val in str_values:
            try:
                converted_val = self.value_converter(str_val)
            except Exception:
                self.fail(f"{str_val} could not be converted by value converter", param, ctx)
            converted_values.append(converted_val)

        return converted_values


class MutuallyExclusiveOption(click.Option):
    """Type for making click options mutually exclusive.

    Usage:
    ```
    @option(
        '--arg-1',
        cls=MutuallyExclusiveOption,
        mutually_exclusive=["arg_2"],
        # ...
    )
    @option(
        '--arg-2',
        cls=MutuallyExclusiveOption,
        mutually_exclusive=["arg_1"],
        # ...
    )
    ```
    """

    def __init__(  # type: ignore
        self,
        *args,
        **kwargs,
    ):
        """Initialize the option.

        mutually_exclusive: A list of strings that indicate incompatible options.
        """
        self.mutually_exclusive = frozenset(kwargs.pop("mutually_exclusive", []))
        if len(self.mutually_exclusive) > 0:
            exclude_str = ",".join(self.mutually_exclusive)
            kwargs["help"] += f" [mutually_exclusive({exclude_str})]"

        super(MutuallyExclusiveOption, self).__init__(*args, **kwargs)

    def handle_parse_result(  # type: ignore  # noqa: D102
        self, ctx: click.Context, opts: Dict[str, Any], args: List[str]
    ) -> Tuple[Any, List[str]]:
        mutually_exclusive_opts_present = len(self.mutually_exclusive.intersection(opts)) > 0

        if mutually_exclusive_opts_present and self.name in opts:
            exclude_str = ",".join(f"'--{opt}'" for opt in self.mutually_exclusive)
            raise click.BadOptionUsage(
                self.name, f"cannot use option '--{self.name}' because it is mutually exclusive with {exclude_str}", ctx
            )

        return super(MutuallyExclusiveOption, self).handle_parse_result(ctx, opts, args)
