from __future__ import annotations

import logging

from typing_extensions import override

from metricflow_semantics.mf_logging.mf_logger import MetricFlowLogger, MetricFlowLoggerFactory
from metricflow_semantics.mf_logging.pretty_print import mf_pformat_many


class PrettyPrintingLogger(MetricFlowLogger):
    """Logger that pretty-prints the objects passed as keyword arguments.

    e.g.
        logger.debug("Found matching path.", matching_path=matching_path, source_nodes=source_nodes)

        ->

        Found matching path.
          matching_path: [1, 2, 3]
          source_nodes: ["a", "b", "c"]
    """

    def __init__(self, python_logger: logging.Logger) -> None:  # noqa: D107
        self._standard_library_logger = python_logger

    @override
    def log_implementation(self, level_int: int, message: str, stack_level: int, **kwargs) -> None:  # type: ignore[no-untyped-def]
        self._standard_library_logger.log(level_int, mf_pformat_many(message, kwargs), stacklevel=stack_level)

    @property
    @override
    def standard_library_logger(self) -> logging.Logger:
        return self._standard_library_logger


class PrettyPrintingLoggerFactory(MetricFlowLoggerFactory):
    """Factory for creating `PrettyPrintingLogger`."""

    @override
    def get_logger(self, module_name: str) -> MetricFlowLogger:
        return PrettyPrintingLogger(logging.getLogger(module_name))
