import copy
import logging

from typing import Sequence

from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.transformations.agg_time_dimension import SetMeasureAggregationTimeDimensionRule
from metricflow.model.transformations.boolean_measure import BooleanMeasureAggregationRule
from metricflow.model.transformations.convert_count import ConvertCountToSumRule
from metricflow.model.transformations.identifiers import CompositeIdentifierExpressionRule
from metricflow.model.transformations.names import LowerCaseNamesRule
from metricflow.model.transformations.proxy_measure import CreateProxyMeasureRule
from metricflow.model.transformations.transform_rule import ModelTransformRule

logger = logging.getLogger(__name__)


class ModelTransformer:
    """Helps to make transformations to a model for convenience.

    Generally used to make it more convenient for the user to develop their model.
    """

    DEFAULT_PRE_VALIDATION_RULES: Sequence[ModelTransformRule] = (
        LowerCaseNamesRule(),
        SetMeasureAggregationTimeDimensionRule(),
    )

    DEFAULT_POST_VALIDATION_RULES: Sequence[ModelTransformRule] = (
        CreateProxyMeasureRule(),
        BooleanMeasureAggregationRule(),
        CompositeIdentifierExpressionRule(),
        ConvertCountToSumRule(),
    )

    @staticmethod
    def pre_validation_transform_model(
        model: UserConfiguredModel, rules: Sequence[ModelTransformRule] = DEFAULT_PRE_VALIDATION_RULES
    ) -> UserConfiguredModel:
        """Transform a model according to configured rules before validations are run."""
        model_copy = copy.deepcopy(model)

        for transform_rule in rules:
            model_copy = transform_rule.transform_model(model_copy)

        return model_copy

    @staticmethod
    def post_validation_transform_model(
        model: UserConfiguredModel, rules: Sequence[ModelTransformRule] = DEFAULT_POST_VALIDATION_RULES
    ) -> UserConfiguredModel:
        """Transform a model according to configured rules after validations are run."""
        model_copy = copy.deepcopy(model)
        for transform_rule in rules:
            model_copy = transform_rule.transform_model(model_copy)

        return model_copy
