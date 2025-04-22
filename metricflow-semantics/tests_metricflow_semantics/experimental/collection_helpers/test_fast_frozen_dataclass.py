from __future__ import annotations

import logging

import pytest
from metricflow_semantics.experimental.test_helpers.performance_helpers import assert_performance_factor
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from pympler import asizeof

from tests_metricflow_semantics.experimental.collection_helpers.fast_frozen_dataclass_test_classes import (
    PATH_TO_FAST_FROZEN_DATACLASS_TEST_CLASSES_PY_FILE,
    FastItem,
    Item,
)
from tests_metricflow_semantics.experimental.collection_helpers.statement_helpers import read_statement_from_path

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def setup_statement() -> str:
    """Setup statement to define dataclasses defined in different ways."""
    return read_statement_from_path(PATH_TO_FAST_FROZEN_DATACLASS_TEST_CLASSES_PY_FILE)


def test_hash(setup_statement: str) -> None:
    """Test that `fast_frozen_dataclass` is faster for repeated hashing."""
    assert_performance_factor(
        slow_code_setup=setup_statement,
        slow_code_statement="hash(left)",
        fast_code_setup=setup_statement,
        fast_code_statement="hash(fast_left)",
        min_performance_factor=10,
    )


def test_in_set(setup_statement: str) -> None:
    """Test that `fast_frozen_dataclass` is faster for repeated set-inclusion checks."""
    assert_performance_factor(
        slow_code_setup=setup_statement,
        slow_code_statement="left in item_group_set",
        fast_code_setup=setup_statement,
        fast_code_statement="fast_left in fast_item_group_set",
        min_performance_factor=10,
    )


def test_in_dict(setup_statement: str) -> None:
    """Test that `fast_frozen_dataclass` is faster for repeated dict-inclusion checks."""
    assert_performance_factor(
        slow_code_setup=setup_statement,
        slow_code_statement="left in item_group_dict",
        fast_code_setup=setup_statement,
        fast_code_statement="fast_left in fast_item_group_dict",
        min_performance_factor=10,
    )


def test_create(setup_statement: str) -> None:
    """Test that `fast_frozen_dataclass` is faster to create."""
    assert_performance_factor(
        slow_code_setup=setup_statement,
        slow_code_statement="create_group('left')",
        fast_code_setup=setup_statement,
        fast_code_statement="create_fast_group('left')",
        min_performance_factor=1.5,
    )


def test_equals(setup_statement: str) -> None:
    """Test that `fast_frozen_dataclass` has similar equals performance."""
    assert_performance_factor(
        slow_code_setup=setup_statement,
        slow_code_statement="left == right",
        fast_code_setup=setup_statement,
        fast_code_statement="fast_left == fast_right",
        # Usually close to 1, but using 0.8 for reduced test flakiness.
        min_performance_factor=0.8,
    )


def test_field_access(setup_statement: str) -> None:
    """Test that `fast_frozen_dataclass` has similar field access performance."""
    assert_performance_factor(
        slow_code_setup=setup_statement,
        slow_code_statement="left.item_group_field_0.item_field_0",
        fast_code_setup=setup_statement,
        fast_code_statement="fast_left.item_group_field_0.item_field_0",
        # Usually close to 1, but using 0.8 for reduced test flakiness.
        min_performance_factor=0.8,
    )


def test_lt(setup_statement: str) -> None:
    """Test that `fast_frozen_dataclass` has similar `<` performance."""
    assert_performance_factor(
        slow_code_setup=setup_statement,
        slow_code_statement="left < right",
        fast_code_setup=setup_statement,
        fast_code_statement="fast_left < fast_right",
        # Usually close to 1, but using 0.8 for reduced test flakiness.
        min_performance_factor=0.8,
    )


@pytest.mark.skip("System dependent.")
def test_size() -> None:
    """Tests the size of the fast frozen dataclass vs frozen dataclass to get a sense of class overheads."""
    left_size = asizeof.asizeof(Item(item_field_0="left"))
    right_size = asizeof.asizeof(FastItem(item_field_0="right"))

    logger.debug(LazyFormat("Computed size comparisons", left_size=left_size, right_size=right_size))
    assert left_size == 288
    assert right_size == 288
