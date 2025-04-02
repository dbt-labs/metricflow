from __future__ import annotations

import logging
from functools import cached_property
from typing import Any, Callable, Mapping, Union

from typing_extensions import override

from metricflow_semantics.mf_logging.pretty_print import mf_pformat_dict

logger = logging.getLogger(__name__)


LoggedObject = Any  # type: ignore[misc]


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

    def __init__(
        self,
        message_title: Union[str, Callable[[], str]],
        **kwargs: Union[LoggedObject, Callable[[], LoggedObject]],
    ) -> None:
        """Initializer.

        Args:
            message_title: The message title or a function that returns a message title. A message is composed of the
            message title and the message body. For this class, the message body is derived from the values of the
            keyword arguments.
            **kwargs: A dictionary of objects to format to text when `__str__` is called on this object. To allow
            for lazy evaluation of argument values, a callable that takes no arguments can be supplied for a key's
            value.
        """
        self._message_title = message_title
        self._kwargs = kwargs

    @cached_property
    def _str_value(self) -> str:
        """Cache the result as `__str__` can be called multiple times for multiple log handlers."""
        if callable(self._message_title):
            message_title = self._message_title()
        else:
            message_title = self._message_title

        return mf_pformat_dict(message_title, self.evaluated_kwargs, preserve_raw_strings=True)

    @override
    def __str__(self) -> str:
        return self._str_value

    @cached_property
    def evaluated_kwargs(self) -> Mapping[str, LoggedObject]:
        """Return the `kwargs` that was used to construct this object, evaluating value if `Callable`.

        This is cached to avoid repeated evaluation.
        """
        evaluated_args = {}
        for arg_name, arg_value in self._kwargs.items():
            if callable(arg_value):
                try:
                    evaluated_args[arg_name] = arg_value()
                    continue
                except Exception:
                    logger.warning(
                        f"Got an exception while evaluating {arg_name=} {arg_value=}. Since this is an error with "
                        f"formatting log output, this should not result in system issues. However, the exception "
                        f"indicates a bug with how the logging call is made and should be investigated.",
                        exc_info=True,
                    )

            evaluated_args[arg_name] = arg_value
        return evaluated_args
