from __future__ import annotations

from metricflow.dataflow.sql_column import SqlColumn
from metricflow.inference.models import InferenceSignal, InferenceSignalConfidence, InferenceSignalType
from metricflow.inference.solver.weighted_tree import WeightedTypeTreeInferenceSolver

column = SqlColumn.from_string("db.schema.table.col")
solver = WeightedTypeTreeInferenceSolver()


def test_empty_signals_return_unknown() -> None:  # noqa: D
    result = solver.solve_column(column, [])

    assert result.type_node == InferenceSignalType.UNKNOWN
    assert len(result.reasons) == 0
    assert len(result.problems) == 2


def test_follow_signal_path() -> None:
    """Test that the solver will return the deepest (most specific) node if it finds a path with multiple signals."""
    signals = [
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.ID.UNIQUE,
            reason="UNIQUE",
            confidence=InferenceSignalConfidence.HIGH,
            only_applies_to_parent=False,
        ),
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.ID.PRIMARY,
            reason="PRIMARY",
            confidence=InferenceSignalConfidence.VERY_HIGH,
            only_applies_to_parent=False,
        ),
    ]

    result = solver.solve_column(column, signals)

    assert result.type_node == InferenceSignalType.ID.PRIMARY
    assert "UNIQUE" in result.reasons[0] and "PRIMARY" in result.reasons[1]


def test_complementary_signal_with_parent_trail() -> None:
    """Test that the solver will follow the weight trail and take complementary signals into account if parent has weight."""
    signals = [
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.ID.UNKNOWN,
            reason="ID",
            confidence=InferenceSignalConfidence.HIGH,
            only_applies_to_parent=False,
        ),
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.ID.UNIQUE,
            reason="UNIQUE",
            confidence=InferenceSignalConfidence.VERY_HIGH,
            only_applies_to_parent=True,
        ),
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.ID.PRIMARY,
            reason="PRIMARY",
            confidence=InferenceSignalConfidence.VERY_HIGH,
            only_applies_to_parent=True,
        ),
    ]

    result = solver.solve_column(column, signals)

    assert result.type_node == InferenceSignalType.ID.PRIMARY
    assert "ID" in result.reasons[0] and "UNIQUE" in result.reasons[1] and "PRIMARY" in result.reasons[2]


def test_complementary_signals_without_parent_signal() -> None:
    """Test that the solver won't follow the weight trail and take complementary signals into account if parent has no weight."""
    signals = [
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.DIMENSION.CATEGORICAL,
            reason="CATEG_DIM",
            confidence=InferenceSignalConfidence.MEDIUM,
            only_applies_to_parent=False,
        ),
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.ID.UNIQUE,
            reason="UNIQUE",
            confidence=InferenceSignalConfidence.VERY_HIGH,
            only_applies_to_parent=True,
        ),
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.ID.PRIMARY,
            reason="PRIMARY",
            confidence=InferenceSignalConfidence.VERY_HIGH,
            only_applies_to_parent=True,
        ),
    ]

    result = solver.solve_column(column, signals)

    assert result.type_node == InferenceSignalType.DIMENSION.CATEGORICAL
    assert "CATEG_DIM" in result.reasons[0]


def test_contradicting_signals() -> None:
    """Test that the solver will return the deepest common ancestor if it finds conflicting signals."""
    signals = [
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.ID.FOREIGN,
            reason="FOREIGN",
            confidence=InferenceSignalConfidence.HIGH,
            only_applies_to_parent=False,
        ),
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.ID.PRIMARY,
            reason="PRIMARY",
            confidence=InferenceSignalConfidence.HIGH,
            only_applies_to_parent=False,
        ),
    ]

    result = solver.solve_column(column, signals)

    assert result.type_node == InferenceSignalType.ID.UNKNOWN


def test_stop_at_internal_node_if_trail_stops() -> None:
    """Test that if the signal trail stops at an internal node the solver will return that node instead of going deeper."""
    signals = [
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.ID.UNKNOWN,
            reason="KEY",
            confidence=InferenceSignalConfidence.HIGH,
            only_applies_to_parent=False,
        ),
        InferenceSignal(
            column=column,
            type_node=InferenceSignalType.ID.UNIQUE,
            reason="UNIQUE",
            confidence=InferenceSignalConfidence.HIGH,
            only_applies_to_parent=False,
        ),
    ]

    result = solver.solve_column(column, signals)

    # should not progress further into the tree and assume it's PRIMARY
    assert result.type_node == InferenceSignalType.ID.UNIQUE
    assert "KEY" in result.reasons[0] and "UNIQUE" in result.reasons[1]
