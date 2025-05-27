from __future__ import annotations

import logging
from abc import ABC, abstractmethod

from typing_extensions import override

from metricflow_semantics.dag.id_prefix import DynamicIdPrefix
from metricflow_semantics.dag.sequential_id import SequentialIdGenerator
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass

logger = logging.getLogger(__name__)


class MetricflowGraphId(ABC):
    @property
    @abstractmethod
    def str_value(self) -> str:
        raise NotImplementedError


@singleton_dataclass()
class UniqueGraphId(MetricflowGraphId):
    _str_value: str

    @staticmethod
    def create() -> UniqueGraphId:
        return UniqueGraphId(
            _str_value=SequentialIdGenerator.create_next_id(DynamicIdPrefix(UniqueGraphId.__class__.__name__)).str_value
        )

    @override
    @property
    def str_value(self) -> str:
        return self.str_value
