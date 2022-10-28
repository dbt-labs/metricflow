import logging
from typing import Tuple, Type

from metricflow.model.dbt_transformations.dbt_transform_rule import DbtTransformRule
from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.materialization import Materialization
from metricflow.model.objects.metric import Metric

logger = logging.getLogger(__name__)

DEFAULT_RULES: Tuple[DbtTransformRule, ...] = tuple()


class DbtTransformer:
    """Handles transforming a dbt Manifest into a UserConfiguredModel"""

    def __init__(  # noqa: D
        self,
        rules: Tuple[DbtTransformRule, ...] = DEFAULT_RULES,
        data_source_class: Type[DataSource] = DataSource,
        metric_class: Type[Metric] = Metric,
        materialization_class: Type[Materialization] = Materialization,
    ) -> None:
        self.rules = rules
        self.data_source_class = data_source_class
        self.metric_class = metric_class
        self.materialization_class = materialization_class
