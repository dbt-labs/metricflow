import copy
import logging

from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.transformations.boolean_measure import BooleanMeasureAggregationRule
from metricflow.model.transformations.identifiers import CompositeIdentifierExpressionRule
from metricflow.model.transformations.names import LowerCaseNamesRule
from metricflow.model.transformations.proxy_measure import CreateProxyMeasureRule

logger = logging.getLogger(__name__)


class ModelTransformer:
    """Helps to make transformations to a model for convenience.

    Generally used to make it more convenient for the user to develop their model.
    """

    @staticmethod
    def pre_validation_transform_model(model: UserConfiguredModel) -> UserConfiguredModel:
        """Transform a model according to configured rules."""
        model_copy = copy.deepcopy(model)
        for transform_rule in (LowerCaseNamesRule(),):
            model_copy = transform_rule.transform_model(model_copy)

        return model_copy

    @staticmethod
    def post_validation_transform_model(model: UserConfiguredModel) -> UserConfiguredModel:
        """Transform a model according to configured rules."""
        model_copy = copy.deepcopy(model)
        for transform_rule in (
            CreateProxyMeasureRule(),
            BooleanMeasureAggregationRule(),
            CompositeIdentifierExpressionRule(),
        ):
            model_copy = transform_rule.transform_model(model_copy)

        return model_copy
