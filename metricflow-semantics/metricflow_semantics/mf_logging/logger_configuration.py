from __future__ import annotations

import inspect
import threading

from metricflow_semantics.mf_logging.mf_logger import MetricFlowLogger, MetricFlowLoggerFactory
from metricflow_semantics.mf_logging.pretty_printing_logger import PrettyPrintingLoggerFactory


class MetricFlowLoggerConfiguration:
    """Global object used to configure logger object creation in MF.

    The factory can be configured to use other implementations to realize different behaviors like passing JSON to the
    `extra` argument. By default, it pretty-formats the objects and appends them to the message.
    """

    _logger: MetricFlowLoggerFactory = PrettyPrintingLoggerFactory()
    _state_lock = threading.Lock()

    @classmethod
    def set_logger_factory(cls, logger_factory: MetricFlowLoggerFactory) -> None:
        """Set the factory to be used for creating logger objects in the `get_logger` method."""
        with cls._state_lock:
            cls._logger = logger_factory

    @classmethod
    def get_logger(cls, module_name: str) -> MetricFlowLogger:
        """Get the logger using the module where the call was made.

        This is a replacement for the standard `logging.getLogger` pattern.

        e.g.
            import logging
            logger = logging.getLogger(__name__)

            ->

            import ...
            logger = MetricFlowLoggerConfiguration.get_module_logger()
        """
        with cls._state_lock:
            return cls._logger.get_logger(module_name)


def mf_get_logger() -> MetricFlowLogger:
    """Get the logger using the module where the call was made.

    This is a replacement for the standard `logging.getLogger` pattern.

    e.g.
        import logging
        logger = logging.getLogger(__name__)

        ->

        import ...
        logger = mf_get_logger()
    """
    caller_frame = inspect.stack()[1]
    caller_module = inspect.getmodule(caller_frame[0])

    if caller_module is None:
        raise RuntimeError(f"Unable to get module for {caller_frame[0]=}")

    return MetricFlowLoggerConfiguration.get_logger(caller_module.__name__)
