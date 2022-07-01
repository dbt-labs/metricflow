import re
from metricflow.dataflow.sql_column import SqlColumn
from metricflow.inference.context.data_warehouse import DataWarehouseInferenceContext
from metricflow.inference.rule.base import InferenceSignalConfidence, InferenceSignalType
from metricflow.inference.rule.rules import ColumnRegexMatcherRule


def test_column_regex_matcher(warehouse_ctx: DataWarehouseInferenceContext):  # noqa: D
    rule = ColumnRegexMatcherRule(
        pattern=re.compile(r"test_column$"),  # have to pass in
        signal_type=InferenceSignalType.DIMENSION,
        confidence=InferenceSignalConfidence.MEDIUM,
    )

    signals = rule.process(warehouse_ctx)

    assert len(signals) == 1
    assert signals[0].confidence == InferenceSignalConfidence.MEDIUM
    assert signals[0].type == InferenceSignalType.DIMENSION
    assert signals[0].column == SqlColumn.from_string("db.schema.table.test_column")
