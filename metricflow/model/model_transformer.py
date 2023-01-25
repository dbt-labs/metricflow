import copy
import logging

from typing import Sequence, Tuple

from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.transformations.add_input_metric_measures import AddInputMetricMeasuresRule
from metricflow.model.transformations.agg_time_dimension import SetMeasureAggregationTimeDimensionRule
from metricflow.model.transformations.boolean_measure import BooleanMeasureAggregationRule
from metricflow.model.transformations.convert_count import ConvertCountToSumRule
from metricflow.model.transformations.convert_median import ConvertMedianToPercentileRule
from metricflow.model.transformations.identifiers import CompositeIdentifierExpressionRule
from metricflow.model.transformations.names import LowerCaseNamesRule
from metricflow.model.transformations.proxy_measure import CreateProxyMeasureRule
from metricflow.model.transformations.transform_rule import ModelTransformRule

logger = logging.getLogger(__name__)


class ModelTransformer:
    """Helps to make transformations to a model for convenience.

    Generally used to make it more convenient for the user to develop their model.
    """

    PRIMARY_RULES: Sequence[ModelTransformRule] = (
        LowerCaseNamesRule(),
        SetMeasureAggregationTimeDimensionRule(),
    )

    SECONDARY_RULES: Sequence[ModelTransformRule] = (
        CreateProxyMeasureRule(),
        BooleanMeasureAggregationRule(),
        CompositeIdentifierExpressionRule(),
        ConvertCountToSumRule(),
        ConvertMedianToPercentileRule(),
        AddInputMetricMeasuresRule(),
    )

    DEFAULT_RULES: Tuple[Sequence[ModelTransformRule], ...] = (
        PRIMARY_RULES,
        SECONDARY_RULES,
    )

    @staticmethod
    def transform(
        model: UserConfiguredModel,
        ordered_rule_sequences: Tuple[Sequence[ModelTransformRule], ...] = DEFAULT_RULES,
    ) -> UserConfiguredModel:
        """Copies the passed in model, applies the rules to the new model, and then returns that model

        It's important to note that some rules need to happen before or after other rules. Thus rules
        are passed in as an ordered tuple of rule sequences. Primary rules are run first, and then
        secondary rules. We don't currently have tertiary, quaternary, or etc currently, but this
        system easily allows for it.
        """
        model_copy = copy.deepcopy(model)

        for rule_sequence in ordered_rule_sequences:
            for rule in rule_sequence:
                model_copy = rule.transform_model(model_copy)

        return model_copy

    @staticmethod
    def pre_validation_transform_model(
        model: UserConfiguredModel, rules: Sequence[ModelTransformRule] = PRIMARY_RULES
    ) -> UserConfiguredModel:
        """Transform a model according to configured rules before validations are run."""
        logger.warning(
            "DEPRECATION: `ModelTransformer.pre_validation_transform_model` is deprecated. Please use `ModelTransformer.transform` instead."
        )

        return ModelTransformer.transform(model=model, ordered_rule_sequences=(rules,))

    @staticmethod
    def post_validation_transform_model(
        model: UserConfiguredModel, rules: Sequence[ModelTransformRule] = SECONDARY_RULES
    ) -> UserConfiguredModel:
        """Transform a model according to configured rules after validations are run."""
        logger.warning(
            "DEPRECATION: `ModelTransformer.post_validation_transform_model` is deprecated. Please use `ModelTransformer.transform` instead."
        )

        return ModelTransformer.transform(model=model, ordered_rule_sequences=(rules,))
