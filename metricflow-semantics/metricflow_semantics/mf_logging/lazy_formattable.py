from __future__ import annotations

from functools import cached_property
from typing import Callable, Union

from typing_extensions import override

from metricflow_semantics.mf_logging.pretty_print import mf_pformat_dict


class LazyFormat:
    """Lazily formats the given objects into a string representation for logging.

    The formatting is done when this object is converted to a string to be compatible with the standard `logging`
    library. This is done lazily as the formatting operation can be expensive and allows debug log statements to not
    incur a performance overhead in production.

    Example:
        logger.debug(LazyFormat(lambda: LazyFormat("Found path", start_point=point_0, end_point=point_1)))

        ->

        DEBUG - Found path
            start_point: Point(x=0.0, y=0.0)
            end_point: Point(x=1.0, y=1.0)

    To aid migration of existing log statements, the message can be a function so that f-strings can be lazily
    evaluated as well. This style will be deprecated as log statements are updated.

    Example:
        logger.debug(LazyFormat(lambda: f"Result is: {expensive_function()}"))

        ->

        logger.debug(LazyFormat(lambda: f"Result is: {expensive_function()}")
    """

    def __init__(self, message: Union[str, Callable[[], str]], **kwargs) -> None:  # type: ignore[no-untyped-def]
        """Initializer.

        Args:
            message: The message or a function that returns a message.
            **kwargs: A dictionary of objects to format to text when `__str__` is called on this object.
        """
        self._message = message
        self._kwargs = kwargs

    @cached_property
    def _str_value(self) -> str:
        """Cache the result as `__str__` can be called multiple times for multiple log handlers."""
        if callable(self._message):
            message = self._message()
        else:
            message = self._message
        return mf_pformat_dict(message, self._kwargs, preserve_raw_strings=True)

    @override
    def __str__(self) -> str:
        return self._str_value
