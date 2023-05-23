from __future__ import annotations

import contextlib
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Iterator, List

import more_itertools

from metricflow.dataflow.sql_table import SqlTable
from metricflow.inference.context.base import InferenceContextProvider
from metricflow.inference.context.data_warehouse import DataWarehouseInferenceContextProvider
from metricflow.inference.renderer.base import InferenceRenderer
from metricflow.inference.rule.base import InferenceRule
from metricflow.inference.solver.base import InferenceSolver

logger = logging.getLogger(__file__)


class InferenceProgressReporter(ABC):
    """Base class for reporting progress while running inference."""

    @staticmethod
    @abstractmethod
    @contextlib.contextmanager
    def warehouse() -> Iterator[None]:
        """Context manager that wraps the warehouse context fetching step."""
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    @contextlib.contextmanager
    def table(table: SqlTable, index: int, total: int) -> Iterator[None]:
        """Context manager that wraps context fetching for a single table."""
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    @contextlib.contextmanager
    def rules() -> Iterator[None]:
        """Context manager that wraps all rule-processing."""
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    @contextlib.contextmanager
    def solver() -> Iterator[None]:
        """Context manager that wraps the solving step."""
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    @contextlib.contextmanager
    def renderers() -> Iterator[None]:
        """Context manager that wraps the rendering step."""
        raise NotImplementedError


class NoOpInferenceProgressReporter(InferenceProgressReporter):
    """Pass-through implementation of `InferenceProgressReporter`."""

    @staticmethod
    @contextlib.contextmanager
    def warehouse() -> Iterator[None]:  # noqa: D
        yield

    @staticmethod
    @contextlib.contextmanager
    def table(table: SqlTable, index: int, total: int) -> Iterator[None]:  # noqa: D
        yield

    @staticmethod
    @contextlib.contextmanager
    def rules() -> Iterator[None]:  # noqa: D
        yield

    @staticmethod
    @contextlib.contextmanager
    def solver() -> Iterator[None]:  # noqa: D
        yield

    @staticmethod
    @contextlib.contextmanager
    def renderers() -> Iterator[None]:  # noqa: D
        yield


# TODO: we still need to add input/output context validations and optimizations.
# Case 1: Rule 1 requires Context of type A, but no provider privides it. Should fail before running
# Case 2: ContextProvider 1 provides Context of type A, but no rule uses it. Should proceed without actually fetching that context.
# Case 3: ContextProviders 1 and 2 both provide Contexts of type A, which is ambiguous. Should fail before running


class InferenceRunner:
    """Glues together all other inference classes in a sequence that actually runs inference."""

    def __init__(
        self,
        context_providers: List[InferenceContextProvider],
        ruleset: List[InferenceRule],
        solver: InferenceSolver,
        renderers: List[InferenceRenderer],
        progress_reporter: InferenceProgressReporter = NoOpInferenceProgressReporter(),
    ) -> None:
        """Initialize the inference runner.

        context_providers: a list of context providers to be used
        ruleset: the set of rules that will produce signals
        solver: the inference solver to be used
        renderers: the renderers that will write inference results as meaningful output
        progress_reporter: `InferenceProgressReporter` to report progress
        """
        logger.warning(
            "Semantic Model Inference is still in Beta. "
            "As such, you should not expect it to be 100% stable or be free of bugs. Any public CLI or Python interfaces may change without prior notice."
            " If you find any bugs or feel like something is not behaving as it should, feel free to open an issue on the Metricflow Github repo."
        )

        if len(context_providers) != 1 or not isinstance(context_providers[0], DataWarehouseInferenceContextProvider):
            raise ValueError("Currently, InferenceRunner requires exactly one DataWarehouseInferenceContextProvider.")

        if len(ruleset) == 0:
            raise ValueError("Running inference with an empty ruleset would produce no result.")

        if len(renderers) == 0:
            raise ValueError("Running inference with no renderer would discard the results.")

        self.context_providers = context_providers
        self.ruleset = ruleset
        self.solver = solver
        self.renderers = renderers
        self._progress = progress_reporter

    def run(self) -> None:
        """Runs inference with the given configs."""
        # FIXME: currently we only accept DataWarehouseContextProvider
        provider: DataWarehouseInferenceContextProvider = self.context_providers[0]  # type: ignore

        with self._progress.warehouse():
            warehouse = provider.get_context(
                table_progress=self._progress.table,
            )

        with self._progress.rules():
            signals_by_column = defaultdict(list)
            signals = [rule.process(warehouse) for rule in self.ruleset]
            for rule_signal in tuple(more_itertools.flatten(signals)):
                signals_by_column[rule_signal.column].append(rule_signal)

        with self._progress.solver():
            results = [self.solver.solve_column(column, signals) for column, signals in signals_by_column.items()]

        with self._progress.renderers():
            for renderer in self.renderers:
                renderer.render(results)
