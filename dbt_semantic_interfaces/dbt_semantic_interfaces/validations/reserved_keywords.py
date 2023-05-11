from typing import List
from dbt_semantic_interfaces.references import SemanticModelElementReference


from dbt_semantic_interfaces.objects.semantic_model import SemanticModel
from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.validations.validator_helpers import (
    SemanticModelContext,
    SemanticModelElementContext,
    SemanticModelElementType,
    FileContext,
    ModelValidationRule,
    ValidationError,
    ValidationIssue,
    validate_safely,
)

# A non-exaustive tuple of reserved keywords
# This list was created by running an intersection of keywords for redshift,
# postgres, bigquery, and snowflake
RESERVED_KEYWORDS = (
    "AND",
    "AS",
    "CREATE",
    "DISTINCT",
    "FOR",
    "FROM",
    "FULL",
    "HAVING",
    "IN",
    "INNER",
    "INTO",
    "IS",
    "JOIN",
    "LEFT",
    "LIKE",
    "NATURAL",
    "NOT",
    "NULL",
    "ON",
    "OR",
    "RIGHT",
    "SELECT",
    "UNION",
    "USING",
    "WHERE",
    "WITH",
)


class ReservedKeywordsRule(ModelValidationRule):
    """Check that any element that ends up being selected by name (instead of expr) isn't a commonly reserved keyword.

    Note: This rule DOES NOT catch all keywords. That is because keywords are
    engine specific, and semantic validations are not engine specific. I.e. if
    you change your underlying data warehouse engine, semantic validations
    should still pass, but your data warehouse validations might fail. However,
    data warehouse validations are slow in comparison to semantic validation
    rules. Thus this rule is intended to catch words that are reserved keywords
    in all supported engines and to fail fast. E.g., `USER` is a reserved keyword
    in Redshift but not in all other supported engines. Therefore if one is
    using Redshift and sets a dimension name to `user`, the config would pass
    this rule, but would then fail Data Warehouse Validations.
    """

    @staticmethod
    @validate_safely(whats_being_done="checking that semantic model sub element names aren't reserved sql keywords")
    def _validate_semantic_model_sub_elements(semantic_model: SemanticModel) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []

        for dimension in semantic_model.dimensions:
            if dimension.name.upper() in RESERVED_KEYWORDS:
                issues.append(
                    ValidationError(
                        context=SemanticModelElementContext(
                            file_context=FileContext.from_metadata(semantic_model.metadata),
                            semantic_model_element=SemanticModelElementReference(
                                semantic_model_name=semantic_model.name, element_name=dimension.name
                            ),
                            element_type=SemanticModelElementType.DIMENSION,
                        ),
                        message=f"'{dimension.name}' is an SQL reserved keyword, and thus cannot be used as a dimension 'name'.",
                    )
                )

        for entity in semantic_model.entities:
            msg = "'{name}' is an SQL reserved keyword, and thus cannot be used as an entity 'name'"
            names = [entity.name]

            for name in names:
                if name.upper() in RESERVED_KEYWORDS:
                    issues.append(
                        ValidationError(
                            context=SemanticModelElementContext(
                                file_context=FileContext.from_metadata(semantic_model.metadata),
                                semantic_model_element=SemanticModelElementReference(
                                    semantic_model_name=semantic_model.name, element_name=entity.name
                                ),
                                element_type=SemanticModelElementType.ENTITY,
                            ),
                            message=msg.format(name=name),
                        )
                    )

        for measure in semantic_model.measures:
            if measure.name.upper() in RESERVED_KEYWORDS:
                issues.append(
                    ValidationError(
                        context=SemanticModelElementContext(
                            file_context=FileContext.from_metadata(semantic_model.metadata),
                            semantic_model_element=SemanticModelElementReference(
                                semantic_model_name=semantic_model.name, element_name=measure.name
                            ),
                            element_type=SemanticModelElementType.MEASURE,
                        ),
                        message=f"'{measure.name}' is an SQL reserved keyword, and thus cannot be used as an measure 'name'.",
                    )
                )

        return issues

    @classmethod
    @validate_safely(whats_being_done="checking that semantic_model node_relations are not sql reserved keywords")
    def _validate_semantic_models(cls, model: SemanticManifest) -> List[ValidationIssue]:
        """Checks names of objects that are not nested."""
        issues: List[ValidationIssue] = []
        set_keywords = set(RESERVED_KEYWORDS)

        for semantic_model in model.semantic_models:
            set_sql_table_path_parts = set(
                [part.upper() for part in semantic_model.node_relation.relation_name.split(".")]
            )
            keyword_intersection = set_keywords.intersection(set_sql_table_path_parts)

            if len(keyword_intersection) > 0:
                issues.append(
                    ValidationError(
                        context=SemanticModelContext(
                            file_context=FileContext.from_metadata(semantic_model.metadata),
                            semantic_model=semantic_model.reference,
                        ),
                        message=f"'{semantic_model.node_relation.relation_name}' contains the SQL reserved keyword(s) {keyword_intersection}, and thus cannot be used for 'node_relation'.",
                    )
                )
            issues += cls._validate_semantic_model_sub_elements(semantic_model=semantic_model)

        return issues

    @classmethod
    @validate_safely(
        whats_being_done="running model validation ensuring elements that aren't selected via a defined expr don't contain reserved keywords"
    )
    def validate_model(cls, model: SemanticManifest) -> List[ValidationIssue]:  # noqa: D
        return cls._validate_semantic_models(model=model)
