from __future__ import annotations

import logging

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.singleton import Singleton
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class PointA(Singleton):
    x: int

    @classmethod
    def get_instance(cls, x: int) -> PointA:
        return cls._get_instance(x=x)


@fast_frozen_dataclass()
class PointB(Singleton):
    x: int

    @classmethod
    def get_instance(cls, x: int) -> PointB:
        return cls._get_instance(x=x)


def test_singleton() -> None:
    a = PointA.get_instance(1)
    b = PointB.get_instance(1)

    logger.info(
        LazyFormat(
            "Created instances",
            a=a,
            b=b,
        )
    )
