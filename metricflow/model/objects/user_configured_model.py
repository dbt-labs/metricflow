from typing import List

from dbt.contracts.graph.nodes import Metric, Entity
from metricflow.model.objects.base import HashableBaseModel


class UserConfiguredModel(HashableBaseModel):
    """Model holds all the information the SemanticLayer needs to render a query"""

    entities: List[Entity]
    metrics: List[Metric]
