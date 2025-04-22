from __future__ import annotations

import logging

import pytest
from metricflow_semantics.experimental.test_helpers.performance_helpers import assert_performance_factor
from metricflow_semantics.helpers.string_helpers import mf_newline_join, mf_dedent

from tests_metricflow_semantics.experimental.collection_helpers.singleton_test_classes import (
    PATH_TO_SINGLETON_TEST_CLASS_PY_FILE,
    SingletonIdElement,
)
from tests_metricflow_semantics.experimental.collection_helpers.statement_helpers import read_statement_from_path

logger = logging.getLogger(__name__)


def test_is_expression() -> None:
    """Test comparison using `is`."""
    left = SingletonIdElement.get_instance(1)
    right = SingletonIdElement.get_instance(1)

    assert left is right


@pytest.fixture(scope="session")
def setup_statement() -> str:
    """Statement that sets up the definition for the classes used in tests below."""
    return read_statement_from_path(PATH_TO_SINGLETON_TEST_CLASS_PY_FILE)


def test_set_equals(setup_statement: str) -> None:
    """Tests performance of set comparison using singletons."""
    size = 4
    assert_performance_factor(
        slow_code_setup=mf_newline_join(
            setup_statement,
            mf_dedent(
                f"""
                left = create_id_set({size})
                right = create_id_set({size})
                """
            ),
        ),
        slow_code_statement="left == right",
        fast_code_setup=mf_newline_join(
            setup_statement,
            mf_dedent(
                f"""
                left = create_singleton_id_set({size})
                right = create_singleton_id_set({size})
                """
            ),
        ),
        fast_code_statement="left == right",
        min_performance_factor=25.0,
    )


def test_set_in(setup_statement: str) -> None:
    """Tests performance of set inclusion checks."""
    size = 10
    assert_performance_factor(
        slow_code_setup=mf_newline_join(
            setup_statement,
            f"id_set = create_id_set({size})",
        ),
        slow_code_statement="FIRST_ID in id_set",
        fast_code_setup=mf_newline_join(
            setup_statement,
            f"singleton_id_set = create_singleton_id_set({size})",
        ),
        fast_code_statement="FIRST_SINGLETON_ID in singleton_id_set",
        min_performance_factor=5.0,
    )


def test_tuple_equals(setup_statement: str) -> None:
    """Tests performance of tuple comparisons."""
    size = 4
    assert_performance_factor(
        slow_code_setup=mf_newline_join(
            setup_statement,
            mf_dedent(
                f"""
                left = create_id_tuple({size})
                right = create_id_tuple({size})
                """
            ),
        ),
        slow_code_statement="left == right",
        fast_code_setup=mf_newline_join(
            setup_statement,
            mf_dedent(
                f"""
                left = create_singleton_id_tuple({size})
                right = create_singleton_id_tuple({size})
                """
            ),
        ),
        fast_code_statement="left == right",
        min_performance_factor=50.0,
    )


def test_create_new(setup_statement: str) -> None:
    """Tests the overhead of creating / getting a new singleton instance.

    Uses a random start index to create new instances instead of getting an existing one.
    """
    size = 1000
    setup_statement = mf_newline_join(setup_statement, "import random")
    assert_performance_factor(
        slow_code_setup=setup_statement,
        slow_code_statement=mf_dedent(
            f"""
            start_index = random.randint(0, 1_000_000_000_000)
            for i in range(start_index, start_index + {size}):
                CompositeId(
                    id_0=IdElement(int_value=i),
                    id_1=IdElement(int_value=i + 1),
                )
            """
        ),
        fast_code_setup=setup_statement,
        fast_code_statement=mf_dedent(
            f"""
            start_index = random.randint(0, 1_000_000_000_000)
            for i in range(start_index, start_index + {size}):
                SingletonCompositeId.get_instance(
                    id_0=SingletonIdElement.get_instance(int_value=i),
                    id_1=SingletonIdElement.get_instance(int_value=i + 1),
                )
            """
        ),
        min_performance_factor=0.15,
    )


def test_create_existing(setup_statement: str) -> None:
    """Tests the overhead of getting a singleton that has already been created."""
    size = 1000
    get_singleton_statement = mf_dedent(
        f"""
        for _ in range({size}):
            SingletonCompositeId.get_instance(
                id_0=SingletonIdElement.get_instance(int_value=0),
                id_1=SingletonIdElement.get_instance(int_value=1),
            )
        """
    )
    assert_performance_factor(
        slow_code_setup=setup_statement,
        slow_code_statement=mf_dedent(
            f"""
            for _ in range({size}):
                CompositeId(
                    id_0=IdElement(int_value=0),
                    id_1=IdElement(int_value=1),
                )
            """
        ),
        fast_code_setup=mf_newline_join(setup_statement, get_singleton_statement),
        fast_code_statement=get_singleton_statement,
        min_performance_factor=0.3,
    )
