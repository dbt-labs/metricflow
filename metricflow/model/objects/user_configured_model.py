from typing import List

from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.materialization import Materialization
from metricflow.model.objects.metric import Metric
from metricflow.model.objects.utils import HashableBaseModel


class UserConfiguredModel(HashableBaseModel):
    """Model holds all the information the SemanticLayer needs to render a query"""

    data_sources: List[DataSource]
    metrics: List[Metric]
    materializations: List[Materialization]
