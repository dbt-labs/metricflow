from __future__ import annotations

import logging
from abc import abstractmethod, ABC
from typing import TypeVar

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.semantic_graph.semantic_graph import MutableSemanticGraph

logger = logging.getLogger(__name__)




@fast_frozen_dataclass()
class RuleInput:
    semantic_graph: MutableSemanticGraph

RuleInputT = TypeVar("RuleInputT", bound=RuleInput, covariant=True)

class SemanticGraphBuilderRule(ABC):

    @abstractmethod
    def update_graph(self, semantic_graph: MutableSemanticGraph) -> None:
        raise NotImplementedError