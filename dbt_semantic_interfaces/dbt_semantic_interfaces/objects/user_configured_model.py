from typing import List

from dbt_semantic_interfaces.objects.data_source import DataSource
from dbt_semantic_interfaces.objects.materialization import Materialization
from dbt_semantic_interfaces.objects.metric import Metric
from dbt_semantic_interfaces.objects.base import HashableBaseModel


class UserConfiguredModel(HashableBaseModel):
    """Model holds all the information the SemanticLayer needs to render a query"""

    data_sources: List[DataSource]
    metrics: List[Metric]
    materializations: List[Materialization] = []
