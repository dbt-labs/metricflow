# from abc import ABC, abstractmethod
# from dataclasses import dataclass
# from typing import Tuple
#
# from dbt_semantic_interfaces.experimental.semantics import SemanticGraph
#
#
# @dataclass(frozen=True)
# class SemanticGraphValidationResult:
#     errors: Tuple[str, ...]
#
#
# class SemanticGraphValidationRule(ABC):
#     @abstractmethod
#     def validate(self, semantic_graph: SemanticGraph) -> SemanticGraphValidationResult:
#         raise NotImplementedError
