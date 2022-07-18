from typing import List

from metricflow.inference.context.data_warehouse import ColumnProperties, InferenceColumnType
from metricflow.inference.models import InferenceSignalConfidence, InferenceSignalType
from metricflow.inference.rule.base import InferenceRule
from metricflow.inference.rule.rules import ColumnMatcherRule


class RuleDefaults:
    """Static factory class for sensible default rules."""

    @staticmethod
    def _any_identifier_matcher(props: ColumnProperties) -> bool:
        """This is the default matcher that is used to determine if columns are identifiers.

        It simply matches column names ending with "id", case insensitive.

        We searched for words ending with "id" just to assess the chance of this resulting in a
        false positive. Our guess is most of those words would rarely, if ever, be used as column names.
        Therefore, not adding a mandatory "_" before "id" would benefit the product by matching names
        like "userid", despite the rare "squid", "mermaid" or "android" matches.

        See: https://www.thefreedictionary.com/words-that-end-in-id
        """
        return props.column.column_name.lower().endswith("id")

    @staticmethod
    def _primary_identifier_matcher(props: ColumnProperties) -> bool:
        """This is the default matcher that is used to determine if columns are primary identifiers.

        It matches columns named "id" or columns named "<table>id" or "<table>_id", where "<table>"
        is the name of the table this column belongs to.
        """
        col_lower = props.column.column_name.lower()
        table_lower = props.column.table_name.lower().rstrip("s")

        if col_lower == "id":
            return True

        return col_lower == f"{table_lower}_id" or col_lower == f"{table_lower}id"

    @staticmethod
    def _unique_identifier_matcher(props: ColumnProperties) -> bool:
        """This is the default matcher to determine if a column is a unique identifier.

        It matches columns with unique values if they are of type INTEGER or STRING.
        """
        return props.distinct_row_count == props.row_count and props.type in [
            InferenceColumnType.INTEGER,
            InferenceColumnType.STRING,
        ]

    @staticmethod
    def _measure_matcher(props: ColumnProperties) -> bool:
        """This is the default matcher that is used to determine if columns are measures.

        It matches columns of type FLOAT.
        """
        return props.type == InferenceColumnType.FLOAT

    @staticmethod
    def _time_dimension_matcher(props: ColumnProperties) -> bool:
        """This is the default matcher that is used to determine if columns are time dimensions.

        It matches columns of type DATETIME.
        """
        return props.type == InferenceColumnType.DATETIME

    @staticmethod
    def default_ruleset() -> List[InferenceRule]:
        """Returns a sensible default set of inference rules."""
        return [
            RuleDefaults.primary_identifier_rule(),
            RuleDefaults.unique_identifier_rule(),
            RuleDefaults.any_identifier_rule(),
            RuleDefaults.measure_rule(),
            RuleDefaults.time_dimension_rule(),
        ]

    @staticmethod
    def primary_identifier_rule() -> ColumnMatcherRule:
        """A default for finding primary identifiers by their names based on column name matches.

        The returned rule will match columns such as `db.schema.mytable.mytable_id`,
        `db.schema.mytable.mytableid` and `db.schema.mytable.id`.

        It will always produce a PRIMARY_IDENTIFIER signal with FOR_SURE confidence.
        """
        return ColumnMatcherRule(
            matcher=RuleDefaults._primary_identifier_matcher,
            type_node=InferenceSignalType.ID.PRIMARY,
            confidence=InferenceSignalConfidence.FOR_SURE,
            match_reason="Column name matches `<table>.id`, `<table>.<table>_id` or `<table>.<table>id`",
        )

    @staticmethod
    def unique_identifier_rule() -> ColumnMatcherRule:
        """A default for finding unique identifier columns.

        The returned rule will match columns with all unique values if their types are either
        INTEGER or STRING.

        It will always produce a UNIQUE_IDENTIFIER signal with HIGH confidence.
        """
        return ColumnMatcherRule(
            matcher=RuleDefaults._unique_identifier_matcher,
            type_node=InferenceSignalType.ID.UNIQUE,
            confidence=InferenceSignalConfidence.HIGH,
            match_reason="Column values are unique.",
        )

    @staticmethod
    def any_identifier_rule() -> ColumnMatcherRule:
        """A default for finding identifiers of any type by their names based on column name matches.

        The returned rule will match columns such as `db.schema.mytable.id`,
        `db.schema.mytable.othertable_id` and `db.schema.mytable.othertableid`

        It will always produce an IDENTIFIER signal with HIGH confidence.
        """
        return ColumnMatcherRule(
            matcher=RuleDefaults._any_identifier_matcher,
            type_node=InferenceSignalType.ID.UNKNOWN,
            confidence=InferenceSignalConfidence.HIGH,
            match_reason="Column name ends with `id`",
        )

    @staticmethod
    def measure_rule() -> ColumnMatcherRule:
        """A default for finding measures based on their type.

        The returned rule will match columns if their type is FLOAT, DOUBLE or any real value.

        It will always prodyce a MEASURE signal with FOR_SURE confidence
        """
        return ColumnMatcherRule(
            matcher=RuleDefaults._measure_matcher,
            type_node=InferenceSignalType.MEASURE.UNKNOWN,
            confidence=InferenceSignalConfidence.FOR_SURE,
            match_reason="Column has real (float) type",
        )

    @staticmethod
    def time_dimension_rule() -> ColumnMatcherRule:
        """A default for finding time dimensions based on their type.

        The returned rule will match columns if their type is DATE, DATETIME, TIMESTAMP or any time value.

        It will always prodce a DIMENSION.TIME signal with FOR_SURE confidence
        """
        return ColumnMatcherRule(
            matcher=RuleDefaults._time_dimension_matcher,
            type_node=InferenceSignalType.DIMENSION.TIME,
            confidence=InferenceSignalConfidence.FOR_SURE,
            match_reason="Column has time type",
        )
