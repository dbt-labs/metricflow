from metricflow.dataflow.sql_column import SqlColumn
from metricflow.inference.rule.base import InferenceSignal, InferenceSignalConfidence, InferenceSignalType
from metricflow.inference.solver.weighted_tree import WeightedTypeTreeInferenceSolver

solver = WeightedTypeTreeInferenceSolver()


def test_empty_signals_return_unknown():  # noqa: D
    type_node, reasons = solver.solve_column([])

    assert type_node == InferenceSignalType.UNKNOWN
    assert len(reasons) == 1


def test_complimentary_signals():
    """Test that `WeightedTypeTreeInferenceSolver` will return the deepest (most specific) node if it finds complimentary signals."""
    column = SqlColumn.from_string("db.schema.table.col")
    signals = [
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.ID.UNIQUE,
            reason="I think it's unique :)",
            confidence=InferenceSignalConfidence.HIGH,
        ),
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.ID.PRIMARY,
            reason="I think it's unique :)",
            confidence=InferenceSignalConfidence.FOR_SURE,
        ),
    ]

    type_node, _ = solver.solve_column(signals)

    assert type_node == InferenceSignalType.ID.PRIMARY


def test_contradicting_signals():
    """Test that `WeightedTypeTreeInferenceSolver` will return the deepest common ancestor if it finds conflicting signals."""
    column = SqlColumn.from_string("db.schema.table.col")
    signals = [
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.ID.FOREIGN,
            reason="I think it's a foreign key :)",
            confidence=InferenceSignalConfidence.HIGH,
        ),
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.ID.PRIMARY,
            reason="I think it's a primary key :)",
            confidence=InferenceSignalConfidence.HIGH,
        ),
    ]

    type_node, _ = solver.solve_column(signals)

    assert type_node == InferenceSignalType.ID.UNKNOWN
