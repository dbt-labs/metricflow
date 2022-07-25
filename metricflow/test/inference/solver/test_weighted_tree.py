from metricflow.dataflow.sql_column import SqlColumn
from metricflow.inference.models import InferenceSignal, InferenceSignalConfidence, InferenceSignalType
from metricflow.inference.solver.weighted_tree import WeightedTypeTreeInferenceSolver

column = SqlColumn.from_string("db.schema.table.col")
solver = WeightedTypeTreeInferenceSolver()


def test_empty_signals_return_unknown():  # noqa: D
    result = solver.solve_column(column, [])

    assert result.type_node == InferenceSignalType.UNKNOWN
    assert len(result.reasons) == 0
    assert len(result.problems) == 2


def test_follow_signal_path():
    """Test that the solver will return the deepest (most specific) node if it finds a path with multiple signals."""
    signals = [
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.ID.UNIQUE,
            reason="UNIQUE",
            confidence=InferenceSignalConfidence.HIGH,
            is_complimentary=False,
        ),
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.ID.PRIMARY,
            reason="PRIMARY",
            confidence=InferenceSignalConfidence.FOR_SURE,
            is_complimentary=False,
        ),
    ]

    result = solver.solve_column(column, signals)

    assert result.type_node == InferenceSignalType.ID.PRIMARY
    assert "UNIQUE" in result.reasons[0] and "PRIMARY" in result.reasons[1]


def test_complimentary_signal_with_parent_trail():
    """Test that the solver will follow the weight trail and take complimentary signals into account if parent has weight"""
    signals = [
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.ID.UNKNOWN,
            reason="ID",
            confidence=InferenceSignalConfidence.HIGH,
            is_complimentary=False,
        ),
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.ID.UNIQUE,
            reason="UNIQUE",
            confidence=InferenceSignalConfidence.FOR_SURE,
            is_complimentary=True,
        ),
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.ID.PRIMARY,
            reason="PRIMARY",
            confidence=InferenceSignalConfidence.FOR_SURE,
            is_complimentary=True,
        ),
    ]

    result = solver.solve_column(column, signals)

    assert result.type_node == InferenceSignalType.ID.PRIMARY
    assert "ID" in result.reasons[0] and "UNIQUE" in result.reasons[1] and "PRIMARY" in result.reasons[2]


def test_complimentary_signals_without_parent_signal():
    """Test that the solver won't follow the weight trail and take complimentary signals into account if parent has no weight"""
    signals = [
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.DIMENSION.CATEGORICAL,
            reason="CATEG_DIM",
            confidence=InferenceSignalConfidence.MEDIUM,
            is_complimentary=False,
        ),
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.ID.UNIQUE,
            reason="UNIQUE",
            confidence=InferenceSignalConfidence.FOR_SURE,
            is_complimentary=True,
        ),
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.ID.PRIMARY,
            reason="PRIMARY",
            confidence=InferenceSignalConfidence.FOR_SURE,
            is_complimentary=True,
        ),
    ]

    result = solver.solve_column(column, signals)

    assert result.type_node == InferenceSignalType.DIMENSION.CATEGORICAL
    assert "CATEG_DIM" in result.reasons[0]


def test_contradicting_signals():
    """Test that the solver will return the deepest common ancestor if it finds conflicting signals."""
    signals = [
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.ID.FOREIGN,
            reason="FOREIGN",
            confidence=InferenceSignalConfidence.HIGH,
            is_complimentary=False,
        ),
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.ID.PRIMARY,
            reason="PRIMARY",
            confidence=InferenceSignalConfidence.HIGH,
            is_complimentary=False,
        ),
    ]

    result = solver.solve_column(column, signals)

    assert result.type_node == InferenceSignalType.ID.UNKNOWN


def test_stop_at_internal_node_if_trail_stops():
    """Test that if the signal trail stops at an internal node the solver will return that node instead of going deeper."""
    signals = [
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.ID.UNKNOWN,
            reason="KEY",
            confidence=InferenceSignalConfidence.HIGH,
            is_complimentary=False,
        ),
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.ID.UNIQUE,
            reason="UNIQUE",
            confidence=InferenceSignalConfidence.HIGH,
            is_complimentary=False,
        ),
    ]

    result = solver.solve_column(column, signals)

    # should not progress further into the tree and assume it's PRIMARY
    assert result.type_node == InferenceSignalType.ID.UNIQUE
    assert "KEY" in result.reasons[0] and "UNIQUE" in result.reasons[1]
