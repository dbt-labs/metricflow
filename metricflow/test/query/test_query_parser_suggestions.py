# Need to ignore flake8 as there are blank lines in the test outputs, which throws "W293 blank line contains whitespace"
# flake8: noqa

import logging
import textwrap

import pytest

from metricflow.model.semantic_model import SemanticModel
from metricflow.query.query_parser import MetricFlowQueryParser
from metricflow.errors.errors import UnableToSatisfyQueryError

logger = logging.getLogger(__name__)


@pytest.fixture(scope="class")
def query_parser(simple_semantic_model: SemanticModel) -> MetricFlowQueryParser:  # noqa: D
    return MetricFlowQueryParser(
        model=simple_semantic_model,
        primary_time_dimension_reference=simple_semantic_model.data_source_semantics.primary_time_dimension_reference,
    )


def test_nonexistent_metric(query_parser: MetricFlowQueryParser) -> None:  # noqa: D
    with pytest.raises(UnableToSatisfyQueryError) as exception_info:
        query_parser.parse_and_validate_query(metric_names=["booking"], group_by_names=["is_instant"])

    assert (
        textwrap.dedent(
            """\
            Unable To Satisfy Query Error: Unknown metric: 'booking'
        
            Suggestions for 'booking':
                ['bookings',
                 'booking_fees',
                 'booking_value',
                 'instant_bookings',
                 'bookings_per_view',
                 'bookers']
            """
        ).rstrip()
        == str(exception_info.value)
    )


def test_non_existent_group_by(query_parser: MetricFlowQueryParser) -> None:  # noqa: D
    with pytest.raises(UnableToSatisfyQueryError) as exception_info:
        query_parser.parse_and_validate_query(metric_names=["bookings"], group_by_names=["is_instan"])

    assert (
        textwrap.dedent(
            """\
            Unable To Satisfy Query Error: Unknown element name 'is_instan' in dimension name 'is_instan'
            
            Suggestions for 'is_instan':
                ['is_instant']
            """
        ).rstrip()
        == str(exception_info.value)
    )


def test_invalid_group_by(query_parser: MetricFlowQueryParser) -> None:  # noqa: D
    with pytest.raises(UnableToSatisfyQueryError) as exception_info:
        query_parser.parse_and_validate_query(metric_names=["bookings"], group_by_names=["capacity_latest"])

    assert (
        textwrap.dedent(
            """\
            Unable To Satisfy Query Error: Dimensions ['capacity_latest'] cannot be resolved for metrics \
['bookings']. The invalid dimension may not exist, require an ambiguous join (e.g. a join path that can be satisfied \
in multiple ways), or require a fanout join.
            
            Suggestions for invalid dimension 'capacity_latest':
                ['listing__capacity_latest', 'listing__country_latest']
            """
        ).rstrip()
        == str(exception_info.value)
    )
