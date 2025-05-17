from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import override

from typing_extensions import override

from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass

logger = logging.getLogger(__name__)


class MetricflowGraphElementId(ABC):
    @property
    @abstractmethod
    def str_value(self) -> str:
        raise NotImplementedError()

    @override
    def __str__(self) -> str:
        return self.str_value


@singleton_dataclass()
class DefaultGraphElementId(MetricflowGraphElementId):
    _str_value: str

    @staticmethod
    def get_instance(str_value: str) -> DefaultGraphElementId:
        return DefaultGraphElementId(_str_value=str_value)

    @override
    @property
    def str_value(self) -> str:
        return self._str_value
