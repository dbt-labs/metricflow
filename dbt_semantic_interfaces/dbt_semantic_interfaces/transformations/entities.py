import logging

from dbt_semantic_interfaces.objects.user_configured_model import UserConfiguredModel
from dbt_semantic_interfaces.transformations.transform_rule import ModelTransformRule

logger = logging.getLogger(__name__)


class CompositeEntityExpressionRule(ModelTransformRule):
    """Transform composite sub-entities for convenience.

    If a sub-entity has no expression, check if an entity exists with the same name and use that entity's
    expression if it has one.
    """

    @staticmethod
    def transform_model(model: UserConfiguredModel) -> UserConfiguredModel:  # noqa: D
        for data_source in model.data_sources:
            for entity in data_source.identifiers:
                if entity.entities is None or len(entity.entities) == 0:
                    continue

                for sub_entity in entity.entities:
                    if sub_entity.name or sub_entity.expr:
                        continue

                    for entity in data_source.identifiers:
                        if sub_entity.ref == entity.name:
                            sub_entity.ref = None
                            sub_entity.name = entity.name
                            sub_entity.expr = entity.expr
                            break

        return model
