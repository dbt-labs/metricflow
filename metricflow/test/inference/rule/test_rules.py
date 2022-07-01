import re
from metricflow.dataflow.sql_column import SqlColumn
from metricflow.dataflow.sql_table import SqlTable
from metricflow.inference.context.data_warehouse import (
    ColumnProperties,
    DataWarehouseInferenceContext,
    InferenceColumnType,
    TableProperties,
)

from metricflow.inference.rule.base import InferenceSignalConfidence, InferenceSignalType
from metricflow.inference.rule.rules import ColumnRegexMatcherRule

warehouse = DataWarehouseInferenceContext(
    table_props=[
        TableProperties(
            table=SqlTable.from_string("db.schema.table"),
            column_props=[
                ColumnProperties(
                    column=SqlColumn.from_string("db.schema.table.asd"),
                    type=InferenceColumnType.INTEGER,
                    row_count=1,
                    distinct_row_count=1,
                    is_nullable=True,
                    null_count=0,
                    min_value=0,
                    max_value=1,
                ),
                ColumnProperties(
                    column=SqlColumn.from_string("db.schema.table.bla"),
                    type=InferenceColumnType.INTEGER,
                    row_count=1,
                    distinct_row_count=1,
                    is_nullable=True,
                    null_count=0,
                    min_value=0,
                    max_value=1,
                ),
            ],
        )
    ]
)


def test_column_regex_matcher():  # noqa: D
    rule = ColumnRegexMatcherRule(
        pattern=re.compile(r".*bla$"),  # have to pass in
        signal_type=InferenceSignalType.DIMENSION,
        confidence=InferenceSignalConfidence.MEDIUM,
    )

    signals = rule.process(warehouse)

    assert len(signals) == 1
    assert signals[0].confidence == InferenceSignalConfidence.MEDIUM
    assert signals[0].type == InferenceSignalType.DIMENSION
    assert signals[0].column == SqlColumn.from_string("db.schema.table.bla")
