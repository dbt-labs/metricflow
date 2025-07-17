# from __future__ import annotations
#
# import logging
# import pathlib
# import typing
# from abc import ABC, abstractmethod
# from collections import defaultdict
# from collections.abc import Mapping, Sequence, Set
# from dataclasses import dataclass
# from enum import Enum
# from pathlib import Path
# from typing import Callable, Generic, Optional, TypeVar
#
# from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
# from metricflow_semantics.experimental.mf_graph.mf_graph import EdgeT
# from metricflow_semantics.experimental.mf_graph.mutable_graph import NodeT
# from metricflow_semantics.experimental.semantic_graph.path_finding.graph_path import MetricflowGraphPath
# from metricflow_semantics.helpers.string_helpers import mf_dedent
# from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
# from metricflow_semantics.mf_logging.pretty_print import mf_pformat, mf_pformat_dict
# from typing_extensions import Self, override
#
# logger = logging.getLogger(__name__)
#
#
# @fast_frozen_dataclass()
# class PathTermination:
#     is_valid: bool
#     description: LazyFormat
#
#
# class PathTerminationCondition(Generic[NodeT, EdgeT], ABC):
#     @abstractmethod
#     def should_terminate(self, path: MetricflowGraphPath[NodeT, EdgeT]) -> Optional[PathTermination]:
#         raise NotImplementedError
#
#
# class WeightLimit(Generic[NodeT, EdgeT], PathTerminationCondition[EdgeT, NodeT]):
#     def __init__(self, weight_limit: int) -> None:
#         self._weight_limit: int = weight_limit
#
#     def should_terminate(self, path: MetricflowGraphPath[NodeT, EdgeT]) -> Optional[PathTermination]:
#         if path.weight > self._weight_limit:
#             return PathTerminationCondition
#         return None
