from __future__ import annotations

from typing import override

from metricflow_semantics.mf_logging.pretty_print import mf_pformat_many


class LazyFormat:
    """Lazily formats the given objects into a string representation for logging.

    The formatting is done when this object is converted to a string to be compatible with the standard `logging`
    library. This is done lazily as the formatting operation can be expensive and allows debug log statements to not
    incur a performance overhead in production.

    Example:
        logger.debug(LazyFormat("Starting drive.", start_point=point_0, end_point=point_1))

        ->

        DEBUG - Starting drive.
            start_point: Point(x=0.0, y=0.0)
            end_point: Point(x=1.0, y=1.0)
    """

    def __init__(self, message: str, **kwargs) -> None:  # type: ignore[no-untyped-def]
        """Initializer.

        Args:
            message: The message to log.
            **kwargs: A dictionary of objects to format to text when `__str__` is called on this object.
        """
        self._message = message
        self._kwargs = kwargs

    @override
    def __str__(self) -> str:
        return mf_pformat_many(self._message, self._kwargs)
