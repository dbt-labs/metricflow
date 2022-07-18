from metricflow.dataflow.sql_column import SqlColumn
from metricflow.inference.context.data_warehouse import ColumnProperties, InferenceColumnType
from metricflow.inference.models import InferenceSignalConfidence, InferenceSignalType
from metricflow.inference.rule.defaults import RuleDefaults
from metricflow.inference.rule.rules import ColumnMatcherRule


def get_column_properties(column_str: str, type: InferenceColumnType, unique: bool) -> ColumnProperties:  # noqa: D
    return ColumnProperties(
        column=SqlColumn.from_string(column_str),
        type=type,
        row_count=10000,
        distinct_row_count=10000 if unique else 9000,
        is_nullable=False,
        null_count=0,
        min_value=0,
        max_value=9999,
    )


def test_any_identifier_matcher():  # noqa: D
    assert RuleDefaults._any_identifier_matcher(
        get_column_properties("db.schema.table.id", InferenceColumnType.INTEGER, True)
    )
    assert RuleDefaults._any_identifier_matcher(
        get_column_properties("db.schema.table.tableid", InferenceColumnType.INTEGER, True)
    )
    assert RuleDefaults._any_identifier_matcher(
        get_column_properties("db.schema.table.table_id", InferenceColumnType.INTEGER, True)
    )
    assert RuleDefaults._any_identifier_matcher(
        get_column_properties("db.schema.table.othertable_id", InferenceColumnType.INTEGER, True)
    )
    assert not RuleDefaults._any_identifier_matcher(
        get_column_properties("db.schema.table.whatever", InferenceColumnType.INTEGER, True)
    )


def test_any_identifier_rule_factory():  # noqa: D
    rule = RuleDefaults.any_identifier_rule()
    assert isinstance(rule, ColumnMatcherRule)
    assert rule.matcher == RuleDefaults._any_identifier_matcher
    assert rule.confidence == InferenceSignalConfidence.HIGH
    assert rule.type_node == InferenceSignalType.ID.UNKNOWN


def test_unique_identifier_matcher():  # noqa: D
    # should match INTEGER or STRING if unique
    assert RuleDefaults._unique_identifier_matcher(
        get_column_properties("db.schema.table.id", InferenceColumnType.INTEGER, True)
    )
    assert RuleDefaults._unique_identifier_matcher(
        get_column_properties("db.schema.table.id", InferenceColumnType.STRING, True)
    )

    # should not match INTEGER or STRING if not unique
    assert not RuleDefaults._unique_identifier_matcher(
        get_column_properties("db.schema.table.id", InferenceColumnType.INTEGER, False)
    )
    assert not RuleDefaults._unique_identifier_matcher(
        get_column_properties("db.schema.table.id", InferenceColumnType.STRING, False)
    )

    # should not match other column types, even if unique
    assert not RuleDefaults._unique_identifier_matcher(
        get_column_properties("db.schema.table.id", InferenceColumnType.DATETIME, True)
    )
    assert not RuleDefaults._unique_identifier_matcher(
        get_column_properties("db.schema.table.id", InferenceColumnType.BOOLEAN, True)
    )
    assert not RuleDefaults._unique_identifier_matcher(
        get_column_properties("db.schema.table.id", InferenceColumnType.FLOAT, True)
    )
    assert not RuleDefaults._unique_identifier_matcher(
        get_column_properties("db.schema.table.id", InferenceColumnType.UNKNOWN, True)
    )


def test_unique_identifier_rule_factory():  # noqa: D
    rule = RuleDefaults.unique_identifier_rule()
    assert isinstance(rule, ColumnMatcherRule)
    assert rule.matcher == RuleDefaults._unique_identifier_matcher
    assert rule.confidence == InferenceSignalConfidence.HIGH
    assert rule.type_node == InferenceSignalType.ID.UNIQUE


def test_primary_identifier_matcher():  # noqa: D
    assert RuleDefaults._primary_identifier_matcher(
        get_column_properties("db.schema.table.id", InferenceColumnType.INTEGER, True)
    )
    assert RuleDefaults._primary_identifier_matcher(
        get_column_properties("db.schema.table.tableid", InferenceColumnType.INTEGER, True)
    )
    assert RuleDefaults._primary_identifier_matcher(
        get_column_properties("db.schema.table.table_id", InferenceColumnType.INTEGER, True)
    )
    assert RuleDefaults._primary_identifier_matcher(
        get_column_properties("db.schema.tables.table_id", InferenceColumnType.INTEGER, True)
    )
    assert RuleDefaults._primary_identifier_matcher(
        get_column_properties("db.schema.tables.tableid", InferenceColumnType.INTEGER, True)
    )
    assert not RuleDefaults._primary_identifier_matcher(
        get_column_properties("db.schema.table.othertable_id", InferenceColumnType.INTEGER, True)
    )
    assert not RuleDefaults._primary_identifier_matcher(
        get_column_properties("db.schema.table.othertableid", InferenceColumnType.INTEGER, True)
    )
    assert not RuleDefaults._primary_identifier_matcher(
        get_column_properties("db.schema.table.whatever", InferenceColumnType.INTEGER, True)
    )


def test_primary_identifier_rule_factory():  # noqa: D
    rule = RuleDefaults.primary_identifier_rule()
    assert isinstance(rule, ColumnMatcherRule)
    assert rule.matcher == RuleDefaults._primary_identifier_matcher
    assert rule.confidence == InferenceSignalConfidence.FOR_SURE
    assert rule.type_node == InferenceSignalType.ID.PRIMARY


def test_measure_matcher():  # noqa: D
    assert RuleDefaults._measure_matcher(get_column_properties("db.schema.table.id", InferenceColumnType.FLOAT, True))

    assert not RuleDefaults._measure_matcher(
        get_column_properties("db.schema.table.id", InferenceColumnType.INTEGER, True)
    )
    assert not RuleDefaults._measure_matcher(
        get_column_properties("db.schema.table.id", InferenceColumnType.DATETIME, True)
    )
    assert not RuleDefaults._measure_matcher(
        get_column_properties("db.schema.table.id", InferenceColumnType.BOOLEAN, True)
    )
    assert not RuleDefaults._measure_matcher(
        get_column_properties("db.schema.table.id", InferenceColumnType.STRING, True)
    )
    assert not RuleDefaults._measure_matcher(
        get_column_properties("db.schema.table.id", InferenceColumnType.UNKNOWN, True)
    )


def test_measure_rule_factory():  # noqa: D
    rule = RuleDefaults.measure_rule()
    assert isinstance(rule, ColumnMatcherRule)
    assert rule.matcher == RuleDefaults._measure_matcher
    assert rule.confidence == InferenceSignalConfidence.FOR_SURE
    assert rule.type_node == InferenceSignalType.MEASURE.UNKNOWN


def test_time_dimension_matcher():  # noqa: D
    assert RuleDefaults._time_dimension_matcher(
        get_column_properties("db.schema.table.id", InferenceColumnType.DATETIME, True)
    )

    assert not RuleDefaults._time_dimension_matcher(
        get_column_properties("db.schema.table.id", InferenceColumnType.INTEGER, True)
    )
    assert not RuleDefaults._time_dimension_matcher(
        get_column_properties("db.schema.table.id", InferenceColumnType.FLOAT, True)
    )
    assert not RuleDefaults._time_dimension_matcher(
        get_column_properties("db.schema.table.id", InferenceColumnType.BOOLEAN, True)
    )
    assert not RuleDefaults._time_dimension_matcher(
        get_column_properties("db.schema.table.id", InferenceColumnType.STRING, True)
    )
    assert not RuleDefaults._time_dimension_matcher(
        get_column_properties("db.schema.table.id", InferenceColumnType.UNKNOWN, True)
    )


def test_time_dimension_rule_factory():  # noqa: D
    rule = RuleDefaults.time_dimension_rule()
    assert isinstance(rule, ColumnMatcherRule)
    assert rule.matcher == RuleDefaults._time_dimension_matcher
    assert rule.confidence == InferenceSignalConfidence.FOR_SURE
    assert rule.type_node == InferenceSignalType.DIMENSION.TIME
