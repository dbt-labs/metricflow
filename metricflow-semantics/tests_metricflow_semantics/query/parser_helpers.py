from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Optional

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.test_helpers.snapshot_helpers import (
    SnapshotConfiguration,
    assert_object_snapshot_equal,
    assert_str_snapshot_equal,
)

logger = logging.getLogger(__name__)


class QueryParserTester:
    """Tester class with convenience methods for checking the result of the query parser."""

    def __init__(  # noqa: D107
        self,
        request: FixtureRequest,
        snapshot_configuration: SnapshotConfiguration,
        semantic_manifest: SemanticManifest,
    ) -> None:
        self._request = request
        self._snapshot_configuration = snapshot_configuration
        self._parser = MetricFlowQueryParser(SemanticManifestLookup(semantic_manifest))

    def assert_error_snapshot(
        self,
        metric_names: Optional[Sequence[str]] = None,
        group_by_names: Optional[Sequence[str]] = None,
        where_constraint_strs: Optional[Sequence[str]] = None,
    ) -> None:
        """Check that the given query raises an error and the error message matches the snapshot."""
        with pytest.raises(Exception) as e:
            self._parser.parse_and_validate_query(
                metric_names=metric_names,
                group_by_names=group_by_names,
                where_constraint_strs=where_constraint_strs,
            )
        assert_str_snapshot_equal(
            request=self._request,
            snapshot_configuration=self._snapshot_configuration,
            snapshot_str=str(e.value),
        )

    def assert_result_snapshot(
        self,
        metric_names: Optional[Sequence[str]] = None,
        group_by_names: Optional[Sequence[str]] = None,
        where_constraint_strs: Optional[Sequence[str]] = None,
    ) -> None:
        """Check that the result of the parser matches the snapshot."""
        result = self._parser.parse_and_validate_query(
            metric_names=metric_names,
            group_by_names=group_by_names,
            where_constraint_strs=where_constraint_strs,
        )
        assert_object_snapshot_equal(
            request=self._request, snapshot_configuration=self._snapshot_configuration, obj=result
        )
