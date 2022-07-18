from metricflow.dataflow.sql_column import SqlColumn
from metricflow.inference.context.data_warehouse import DataWarehouseInferenceContext
from metricflow.inference.models import InferenceSignalConfidence, InferenceSignalType
from metricflow.inference.rule.rules import ColumnMatcher, ColumnMatcherRule


def test_column_matcher(warehouse_ctx: DataWarehouseInferenceContext):  # noqa: D
    matcher: ColumnMatcher = lambda props: props.column.column_name.endswith("test_column")
    rule = ColumnMatcherRule(
        matcher=matcher,  # have to pass in
        type_node=InferenceSignalType.DIMENSION.UNKNOWN,
        confidence=InferenceSignalConfidence.MEDIUM,
        match_reason="test reason",
    )

    signals = rule.process(warehouse_ctx)

    assert len(signals) == 1
    assert signals[0].confidence == InferenceSignalConfidence.MEDIUM
    assert signals[0].type_node == InferenceSignalType.DIMENSION.UNKNOWN
    assert signals[0].column == SqlColumn.from_string("db.schema.table.test_column")
    assert signals[0].reason == "test reason"
