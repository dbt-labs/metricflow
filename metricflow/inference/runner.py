from __future__ import annotations
from collections import defaultdict

from typing import List, Optional

from metricflow.errors.errors import InferenceError
from metricflow.inference.context.base import InferenceContextProvider
from metricflow.inference.rule.base import InferenceRule, InferenceSignal
from metricflow.inference.solver.base import InferenceSolver
from metricflow.inference.renderer.base import InferenceRenderer
from metricflow.inference.models import InferenceResult

# TODO: we still need to add input/output context validations and optimizations.
# Case 1: Rule 1 requires Context of type A, but no provider privides it. Should fail before running
# Case 2: ContextProvider 1 provides Context of type A, but no rule uses it. Should proceed without actually fetching that context.
# Case 3: ContextProviders 1 and 2 both provide Contexts of type A, which is ambiguous. Should fail before running


class InferenceRunner:
    """Glues together all other inference classes in a sequence that actually runs inference."""

    def __init__(self) -> None:  # noqa: D
        self.context_providers: List[InferenceContextProvider] = []
        self.ruleset: List[InferenceRule] = []
        self.solver: Optional[InferenceSolver] = None
        self.renderers: List[InferenceRenderer] = []

    def set_ruleset(self, ruleset: List[InferenceRule]) -> InferenceRunner:
        """Configure the ruleset to be used during inference."""
        self.ruleset = ruleset
        return self

    def add_rule(self, rule: InferenceRule) -> InferenceRunner:
        """Add a rule to this runner's ruleset."""
        self.ruleset.append(rule)
        return self

    def add_context_provider(self, provider: InferenceContextProvider) -> InferenceRunner:
        """Add a context provider to be used during inference"""
        self.context_providers.append(provider)
        return self

    def set_solver(self, solver: InferenceSolver) -> InferenceRunner:
        """Set the solver to be used during inference"""
        self.solver = solver
        return self

    def add_renderer(self, renderer: InferenceRenderer) -> InferenceRunner:
        """Add a renderer that writes inference results to an output"""
        self.renderers.append(renderer)
        return self

    def run(self) -> None:
        """Runs inference with the given configs."""
        if len(self.context_providers) != 1:
            raise InferenceError(
                "Currently, InferenceRunner requires exactly one DataWarehouseInferenceContextProvider."
            )

        if len(self.ruleset) == 0:
            raise InferenceError("Running inference with an empty ruleset would produce no result.")

        if self.solver is None:
            raise InferenceError("Cannot run inference without a solver.")

        if len(self.renderers) == 0:
            raise InferenceError("Running inference with no renderer would discard the results.")

        warehouse = self.context_providers[0].get_context()
        signals: List[InferenceSignal] = []
        for rule in self.ruleset:
            rule_signals = rule.process(warehouse)
            for rule_signal in rule_signals:
                signals.append(rule_signal)

        signals_by_column = defaultdict(list)
        for signal in signals:
            signals_by_column[signal.column].append(signal)

        solved_columns = {column: self.solver.solve_column(signals) for column, signals in signals_by_column.items()}

        results = [
            InferenceResult(column=column, type_node=type_node, reasons=reasons)
            for column, (type_node, reasons) in solved_columns.items()
        ]

        for renderer in self.renderers:
            renderer.render(results)
