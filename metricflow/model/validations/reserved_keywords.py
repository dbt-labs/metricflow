from typing import List
from metricflow.dataflow.sql_table import SqlTable
from metricflow.instances import DataSourceElementReference


from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.validator_helpers import (
    DataSourceContext,
    DataSourceElementContext,
    DataSourceElementType,
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
    @validate_safely(whats_being_done="checking that data source sub element names aren't reserved sql keywords")
    def _validate_data_source_sub_elements(data_source: DataSource) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []

        for dimension in data_source.dimensions:
            if dimension.name.upper() in RESERVED_KEYWORDS:
                issues.append(
                    ValidationError(
                        context=DataSourceElementContext(
                            file_context=FileContext.from_metadata(data_source.metadata),
                            data_source_element=DataSourceElementReference(
                                data_source_name=data_source.name, element_name=dimension.name
                            ),
                            element_type=DataSourceElementType.DIMENSION,
                        ),
                        message=f"'{dimension.name}' is an SQL reserved keyword, and thus cannot be used as a dimension 'name'.",
                    )
                )

        for identifier in data_source.identifiers:
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
                            context=DataSourceElementContext(
                                file_context=FileContext.from_metadata(data_source.metadata),
                                data_source_element=DataSourceElementReference(
                                    data_source_name=data_source.name, element_name=identifier.name
                                ),
                                element_type=DataSourceElementType.IDENTIFIER,
                            ),
                            message=msg.format(name=name),
                        )
                    )

        for measure in data_source.measures:
            if measure.name.upper() in RESERVED_KEYWORDS:
                issues.append(
                    ValidationError(
                        context=DataSourceElementContext(
                            file_context=FileContext.from_metadata(data_source.metadata),
                            data_source_element=DataSourceElementReference(
                                data_source_name=data_source.name, element_name=measure.name
                            ),
                            element_type=DataSourceElementType.MEASURE,
                        ),
                        message=f"'{measure.name}' is an SQL reserved keyword, and thus cannot be used as an measure 'name'.",
                    )
                )

        return issues

    @classmethod
    @validate_safely(whats_being_done="checking that data_source sql_tables are not sql reserved keywords")
    def _validate_data_sources(cls, model: UserConfiguredModel) -> List[ValidationIssueType]:
        """Checks names of objects that are not nested."""
        issues: List[ValidationIssueType] = []
        set_keywords = set(RESERVED_KEYWORDS)

        for data_source in model.data_sources:
            if data_source.sql_table is not None:
                set_sql_table_path_parts = set(
                    [part.upper() for part in SqlTable.from_string(data_source.sql_table).parts_tuple]
                )
                keyword_intersection = set_keywords.intersection(set_sql_table_path_parts)

                if len(keyword_intersection) > 0:
                    issues.append(
                        ValidationError(
                            context=DataSourceContext(
                                file_context=FileContext.from_metadata(data_source.metadata),
                                data_source=data_source.reference,
                            ),
                            message=f"'{data_source.sql_table}' contains the SQL reserved keyword(s) {keyword_intersection}, and thus cannot be used for 'sql_table'.",
                        )
                    )
            issues += cls._validate_data_source_sub_elements(data_source=data_source)

        return issues

    @classmethod
    @validate_safely(
        whats_being_done="running model validation ensuring elements that aren't selected via a defined expr don't contain reserved keywords"
    )
    def validate_model(cls, model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        return cls._validate_data_sources(model=model)
