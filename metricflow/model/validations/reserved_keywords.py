from typing import List
from metricflow.dataflow.sql_table import SqlTable
from metricflow.instances import MetricFlowEntityElementReference


from metricflow.model.objects.conversions import MetricFlowMetricFlowEntity
from dbt.contracts.graph.manifest import UserConfiguredModel
from metricflow.model.validations.validator_helpers import (
    MetricFlowEntityContext,
    MetricFlowEntityElementContext,
    MetricFlowEntityElementType,
    FileContext,
    ModelValidationRule,
    ValidationError,
    ValidationIssueType,
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
    @validate_safely(whats_being_done="checking that entity sub element names aren't reserved sql keywords")
    def _validate_entity_sub_elements(entity: MetricFlowEntity) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []

        for dimension in entity.dimensions:
            if dimension.name.upper() in RESERVED_KEYWORDS:
                issues.append(
                    ValidationError(
                        context=MetricFlowEntityElementContext(
                            file_context=FileContext.from_metadata(entity.metadata),
                            entity_element=MetricFlowEntityElementReference(
                                entity_name=entity.name, element_name=dimension.name
                            ),
                            element_type=MetricFlowEntityElementType.DIMENSION,
                        ),
                        message=f"'{dimension.name}' is an SQL reserved keyword, and thus cannot be used as a dimension 'name'.",
                    )
                )

        for identifier in entity.identifiers:
            if identifier.is_composite:
                msg = "'{name}' is an SQL reserved keyword, and thus cannot be used as a sub-identifier 'name'"
                names = [sub_ident.name for sub_ident in identifier.identifiers if sub_ident.name is not None]
            else:
                msg = "'{name}' is an SQL reserved keyword, and thus cannot be used as an identifier 'name'"
                names = [identifier.name]

            for name in names:
                if name.upper() in RESERVED_KEYWORDS:
                    issues.append(
                        ValidationError(
                            context=MetricFlowEntityElementContext(
                                file_context=FileContext.from_metadata(entity.metadata),
                                entity_element=MetricFlowEntityElementReference(
                                    entity_name=entity.name, element_name=identifier.name
                                ),
                                element_type=MetricFlowEntityElementType.IDENTIFIER,
                            ),
                            message=msg.format(name=name),
                        )
                    )

        for measure in entity.measures:
            if measure.name.upper() in RESERVED_KEYWORDS:
                issues.append(
                    ValidationError(
                        context=MetricFlowEntityElementContext(
                            file_context=FileContext.from_metadata(entity.metadata),
                            entity_element=MetricFlowEntityElementReference(
                                entity_name=entity.name, element_name=measure.name
                            ),
                            element_type=MetricFlowEntityElementType.MEASURE,
                        ),
                        message=f"'{measure.name}' is an SQL reserved keyword, and thus cannot be used as an measure 'name'.",
                    )
                )

        return issues

    @classmethod
    @validate_safely(whats_being_done="checking that entity sql_tables are not sql reserved keywords")
    def _validate_entities(cls, model: UserConfiguredModel) -> List[ValidationIssueType]:
        """Checks names of objects that are not nested."""
        issues: List[ValidationIssueType] = []
        set_keywords = set(RESERVED_KEYWORDS)

        for entity in model.entities:
            if entity.sql_table is not None:
                set_sql_table_path_parts = set(
                    [part.upper() for part in SqlTable.from_string(entity.sql_table).parts_tuple]
                )
                keyword_intersection = set_keywords.intersection(set_sql_table_path_parts)

                if len(keyword_intersection) > 0:
                    issues.append(
                        ValidationError(
                            context=MetricFlowEntityContext(
                                file_context=FileContext.from_metadata(entity.metadata),
                                entity=entity.reference,
                            ),
                            message=f"'{entity.sql_table}' contains the SQL reserved keyword(s) {keyword_intersection}, and thus cannot be used for 'sql_table'.",
                        )
                    )
            issues += cls._validate_entity_sub_elements(entity=entity)

        return issues

    @classmethod
    @validate_safely(
        whats_being_done="running model validation ensuring elements that aren't selected via a defined expr don't contain reserved keywords"
    )
    def validate_model(cls, model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        return cls._validate_entities(model=model)
