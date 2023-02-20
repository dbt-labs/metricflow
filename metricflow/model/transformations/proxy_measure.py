import logging

from metricflow.errors.errors import ModelTransformError
from dbt.contracts.graph.metrics import MetricInputMeasure, MetricType, MetricTypeParams
from dbt.contracts.graph.nodes import Metric
from dbt.contracts.graph.manifest import UserConfiguredModel
from metricflow.model.transformations.transform_rule import ModelTransformRule

logger = logging.getLogger(__name__)


class CreateProxyMeasureRule(ModelTransformRule):
    """Adds a proxy metric for measures that have the create_metric flag set, if it does not already exist.

    Also checks that a defined metric with the same name as a measure is a proxy metric.
    """

    @staticmethod
    def transform_model(model: UserConfiguredModel) -> UserConfiguredModel:
        """Creates measure proxy metrics for measures with `create_metric==True`"""
        for entity in model.entities:
            for measure in entity.measures:
                if not measure.create_metric:
                    continue

                add_metric = True
                for metric in model.metrics:
                    if metric.name == measure.name:
                        if metric.type != MetricType.MEASURE_PROXY:
                            raise ModelTransformError(
                                f"Cannot have metric with the same name as a measure ({measure.name}) that is not a "
                                f"proxy for that measure"
                            )
                        logger.warning(
                            f"Metric already exists with name ({measure.name}). *Not* adding measure proxy metric for "
                            f"that measure"
                        )
                        add_metric = False

                if add_metric is True:
                    model.metrics.append(
                        Metric(
                            name=measure.name,
                            type=MetricType.MEASURE_PROXY,
                            type_params=MetricTypeParams(
                                measures=[MetricInputMeasure(name=measure.name)],
                                expr=measure.name,
                            ),
                        )
                    )

        return model
