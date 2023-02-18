from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod

from pydantic import BaseModel
from dbt.contracts.graph.nodes import Entity, Metric


class HashableBaseModel(BaseModel):
    """Extends BaseModel with a generic hash function"""

    def __hash__(self) -> int:  # noqa: D
        return hash(json.dumps(self.json(sort_keys=True), sort_keys=True))


class MetricFlowMetricFlowEntity(Entity,HashableBaseModel):
    """Extends dbt MetricFlowEntity with Pydantic functionalirt"""

    pass

class MetricFlowMetric(Metric,HashableBaseModel):
    """Extends dbt MetricFlowEntity with Pydantic functionalirt"""

    pass