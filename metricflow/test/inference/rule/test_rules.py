from metricflow.dataflow.sql_column import SqlColumn
from metricflow.inference.context.data_warehouse import DataWarehouseInferenceContext, ColumnProperties
from metricflow.inference.models import InferenceSignalConfidence, InferenceSignalType
from metricflow.inference.rule.rules import ColumnMatcherRule


def test_column_matcher(warehouse_ctx: DataWarehouseInferenceContext):  # noqa: D
    class TestRule(ColumnMatcherRule):
        type_node = InferenceSignalType.DIMENSION.UNKNOWN
        confidence = InferenceSignalConfidence.MEDIUM
        match_reason = "test reason"
        complimentary_signal = False

        def match_column(self, props: ColumnProperties) -> bool:
            return props.column.column_name.endswith("test_column")

    signals = TestRule().process(warehouse_ctx)

    assert len(signals) == 1
    assert signals[0].confidence == InferenceSignalConfidence.MEDIUM
    assert signals[0].type_node == InferenceSignalType.DIMENSION.UNKNOWN
    assert signals[0].column == SqlColumn.from_string("db.schema.table.test_column")
    assert signals[0].reason == "test reason"
    assert not signals[0].is_complimentary
