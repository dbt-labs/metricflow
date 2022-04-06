import logging

from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.transformations.transform_rule import ModelTransformRule

logger = logging.getLogger(__name__)


class CompositeIdentifierExpressionRule(ModelTransformRule):
    """Transform composite sub-identifiers for convenience.

    If a sub-identifier has no expression, check if an identifier exists with the same name and use that identifier's
    expression if it has one.
    """

    @staticmethod
    def transform_model(model: UserConfiguredModel) -> UserConfiguredModel:  # noqa: D
        for data_source in model.data_sources:
            for identifier in data_source.identifiers:
                if identifier.identifiers is None or len(identifier.identifiers) == 0:
                    continue

                for sub_identifier in identifier.identifiers:
                    if sub_identifier.name or sub_identifier.expr:
                        continue

                    for identifier in data_source.identifiers:
                        if sub_identifier.ref == identifier.name.element_name:
                            sub_identifier.ref = None
                            sub_identifier.name = identifier.name
                            sub_identifier.expr = identifier.expr
                            break

        return model
