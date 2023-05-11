from typing import List

from dbt_semantic_interfaces.objects.semantic_model import SemanticModel
from dbt_semantic_interfaces.objects.metric import Metric
from dbt_semantic_interfaces.objects.base import HashableBaseModel


class SemanticManifest(HashableBaseModel):
    """Model holds all the information the SemanticLayer needs to render a query"""

    semantic_models: List[SemanticModel]
    metrics: List[Metric]
