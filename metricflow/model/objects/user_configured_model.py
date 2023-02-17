from typing import List

from metricflow.model.objects.entity import Entity
from metricflow.model.objects.metric import Metric
from metricflow.model.objects.base import HashableBaseModel


class UserConfiguredModel(HashableBaseModel):
    """Model holds all the information the SemanticLayer needs to render a query"""

    entities: List[Entity]
    metrics: List[Metric]
