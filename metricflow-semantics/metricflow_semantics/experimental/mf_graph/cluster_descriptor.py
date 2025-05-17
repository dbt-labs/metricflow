from __future__ import annotations

import logging
from abc import abstractmethod

from typing_extensions import override

from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass

logger = logging.getLogger(__name__)


class MetricflowGraphClusterId:
    @property
    @abstractmethod
    def dot_name(self) -> str:
        raise NotImplementedError()


@singleton_dataclass()
class StringClusterId(MetricflowGraphClusterId):
    str_value: str

    @override
    @property
    def dot_name(self) -> str:
        return self.str_value
