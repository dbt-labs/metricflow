from abc import ABC, abstractmethod
from typing import List, Tuple

from metricflow.inference.models import InferenceSignal, InferenceSignalNode


class InferenceSolver(ABC):
    """Base class for inference solvers.

    Inference solvers implement algorithms for making a final decision over what a column is or isn't, based on
    signals produced by inference rules for that column.
    """

    @abstractmethod
    def solve_column(self, signals: List[InferenceSignal]) -> Tuple[InferenceSignalNode, List[str]]:
        """Make a decision about a column based on its input signals.

        Returns a definitive type for the column and a list of human-readable reasons for this decision.
        """
        raise NotImplementedError
