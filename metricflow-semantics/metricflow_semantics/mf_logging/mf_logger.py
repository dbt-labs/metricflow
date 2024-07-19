from __future__ import annotations

import logging
from abc import ABC, abstractmethod


class MetricFlowLogger(ABC):
    """Logger backed by the Python standard-library logger to conveniently and efficiently log objects.

    In MF, there is a need to log complex objects like DAGs and paths for debugging. Previously, the pattern for logging
    those objects used a pretty-formatting method like:

        logger = logging.getLogger(__name__)
        ...
        logger.debug(mf_pformat_many("Found matching path.", {"matching_path": matching_path, "source_nodes": source_nodes}))

        ->

        Found matching path.
          matching_path: [1, 2, 3]
          source_nodes: ["a", "b", "c"]

    However, there were a few issues:

    * The `mf_pformat_many()` call was evaluated regardless of whether the given logging level was enabled. The
      formatting of complex objects was found to be significant in performance profiling.
    * Since the message is the only object passed to the logger, it was not reasonable to convert the objects to a
      different form (e.g. JSON) for different backends.
    * Logging calls were verbose, and string keys like "matching_path" were not amenable to auto-complete in the IDE.

    The class is used in a new approach that uses a custom logging class that takes keyword arguments that are only
    converted to strings when the corresponding logging level is enabled. The logger is created via a configurable
    factory that can be used to implement different ways of logging those objects (e.g. JSON).

    The new pattern addresses the above issues and looks like:

        logger = mf_get_logger()
        ...
        logger.debug("Found matching path.", matching_path=matching_path, source_nodes=source_nodes)

    A signature for the logging calls that allows non-named arguments and uses the variable names as keys in the output
    was considered for reduced verbosity:

        logger.debug("Found matching path.", matching_path, source_nodes)

    This approach was prototyped using the `varname` library to resolve the variable name, but it was found to be too
    slow (~0.7s for 1000 calls).
    """

    @abstractmethod
    def log_implementation(self, level_int: int, message: str, stack_level: int, **kwargs) -> None:  # type: ignore[no-untyped-def]
        """Appropriately log the message and the appropriate representation of `kwargs`.

        An example of the representation of `kwargs` might be the string form of the objects on separate lines.
        The `stack_level` argument should be passed to the Python-standard-library log-call so that the correct file
        and line number are used for the source of the log line, not this library.
        """
        raise NotImplementedError

    def _safely_log(self, level_int: int, message: str, **kwargs) -> None:  # type: ignore[no-untyped-def]
        """Calls `.log_implementation` with a try-except so that bugs do not cause a fatal exit."""
        if not self.standard_library_logger.isEnabledFor(level_int):
            return
        try:
            self.log_implementation(level_int, message, stack_level=4, **kwargs)
        except Exception:
            logging.exception(
                f"Got an exception while using logger instance {self}. "
                f"Since there might be an issue with the logger instance, this message is logged using the root logger."
            )

    def debug(self, message: str, **kwargs) -> None:  # type: ignore[no-untyped-def]  # noqa: D102
        self._safely_log(logging.DEBUG, message, **kwargs)

    def info(self, message: str, **kwargs) -> None:  # type: ignore[no-untyped-def]  # noqa: D102
        self._safely_log(logging.INFO, message, **kwargs)

    def warning(self, message: str, **kwargs) -> None:  # type: ignore[no-untyped-def]  # noqa: D102
        self._safely_log(logging.WARNING, message, **kwargs)

    def error(self, message: str, **kwargs) -> None:  # type: ignore[no-untyped-def]  # noqa: D102
        self._safely_log(logging.ERROR, message, **kwargs)

    def critical(self, message: str, **kwargs) -> None:  # type: ignore[no-untyped-def]  # noqa: D102
        self._safely_log(logging.CRITICAL, message, **kwargs)

    def fatal(self, message: str, **kwargs) -> None:  # type: ignore[no-untyped-def]  # noqa: D102
        self._safely_log(logging.FATAL, message, **kwargs)

    @property
    @abstractmethod
    def standard_library_logger(self) -> logging.Logger:
        """Return the Python-standard-library logger that this uses."""
        raise NotImplementedError


class MetricFlowLoggerFactory(ABC):
    """Interface for creating logger instances."""

    @abstractmethod
    def get_logger(self, module_name: str) -> MetricFlowLogger:
        """Create a logger for the given module. Similar to the standard `logging.getLogger(__name__)`."""
        raise NotImplementedError
