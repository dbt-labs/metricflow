from __future__ import annotations

import logging
from functools import cached_property
from typing import Any, Callable, Mapping, Union

from typing_extensions import override

from metricflow_semantics.toolkit.mf_logging.pretty_print import PrettyFormatDictOption, mf_pformat_dict

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
    def evaluated_value(self) -> str:
        """Cache the result as `__str__` can be called multiple times for multiple log handlers."""
        return mf_pformat_dict(
            self.evaluated_message_title,
            self.evaluated_kwargs,
            format_option=PrettyFormatDictOption(preserve_raw_strings=True),
        )

    @override
    def __str__(self) -> str:
        return self.evaluated_value

    @cached_property
    def evaluated_kwargs(self) -> Mapping[str, LoggedObject]:
        """Return the `kwargs` that was used to construct this object, evaluating value if `Callable`.

        This is cached for similar reasons as `evaluated_value`.
        """
        evaluated_args = {}
        for arg_name, arg_value in self._kwargs.items():
            if callable(arg_value):
                try:
                    evaluated_args[arg_name] = arg_value()
                    continue
                except Exception:
                    logger.warning(
                        "Got an exception while evaluating an argument (see `extra` for this log record). Since"
                        " this is an error with formatting log output, this should not result in system issues."
                        " However, the exception indicates a bug with how the logging call is made and should be"
                        " investigated.",
                        exc_info=True,
                        extra={"arg_name": arg_name, "arg_value": arg_value},
                    )

            evaluated_args[arg_name] = arg_value
        return evaluated_args

    @cached_property
    def evaluated_message_title(self) -> str:
        """Return the `message_title` that was used to construct this object, evaluating value if `Callable`.

        This is cached for similar reasons as `evaluated_value`.
        """
        if callable(self._message_title):
            # May be helpful to wrap this with a try/except as similar to `evaluated_kwargs`.
            return self._message_title()
        else:
            return self._message_title
