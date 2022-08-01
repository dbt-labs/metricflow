from typing import Callable, Generic, List, Optional, TypeVar
import click

T = TypeVar("T")


class ListParamType(click.ParamType, Generic[T]):
    """A click parameter that is a list of elements.

    It can be used to parse strings like "1,2,4,8" into a List[int], for example.
    """

    name = "list"

    def __init__(
        self,
        value_converter: Callable[[str], T] = lambda x: x,  # type: ignore
        min_length: int = 0,
        max_length: Optional[int] = None,
        separator: str = ",",
        *args,
        **kwargs,
    ) -> None:
        """Initialize the list param type.

        converter: a function to convert each list value.
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

    def convert(self, value: str, param: Optional[click.Parameter], ctx: Optional[click.Context]) -> List[T]:  # noqa: D
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
