from __future__ import annotations

import logging
from typing import Mapping

import pytest
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest

from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def simple_semantic_manifest_lookup_non_ds(  # noqa: D
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestName, MetricFlowEngineTestFixture]
) -> SemanticManifestLookup:
    return mf_engine_test_fixture_mapping[SemanticManifestName.NON_SM_MANIFEST].semantic_manifest_lookup


@pytest.fixture(scope="session")
def simple_semantic_manifest_lookup(  # noqa: D
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestName, MetricFlowEngineTestFixture]
) -> SemanticManifestLookup:
    return mf_engine_test_fixture_mapping[SemanticManifestName.SIMPLE_MANIFEST].semantic_manifest_lookup


@pytest.fixture(scope="session")
def partitioned_multi_hop_join_semantic_manifest_lookup(  # noqa: D
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestName, MetricFlowEngineTestFixture]
) -> SemanticManifestLookup:
    return mf_engine_test_fixture_mapping[
        SemanticManifestName.PARTITIONED_MULTI_HOP_JOIN_MANIFEST
    ].semantic_manifest_lookup


@pytest.fixture(scope="session")
def multi_hop_join_semantic_manifest_lookup(  # noqa: D
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestName, MetricFlowEngineTestFixture]
) -> SemanticManifestLookup:
    return mf_engine_test_fixture_mapping[SemanticManifestName.MULTI_HOP_JOIN_MANIFEST].semantic_manifest_lookup


@pytest.fixture(scope="session")
def simple_semantic_manifest(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestName, MetricFlowEngineTestFixture]
) -> SemanticManifest:
    """Model used for many tests."""
    return mf_engine_test_fixture_mapping[SemanticManifestName.SIMPLE_MANIFEST].semantic_manifest


@pytest.fixture(scope="session")
def extended_date_semantic_manifest_lookup(  # noqa: D
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestName, MetricFlowEngineTestFixture]
) -> SemanticManifestLookup:
    return mf_engine_test_fixture_mapping[SemanticManifestName.EXTENDED_DATE_MANIFEST].semantic_manifest_lookup


@pytest.fixture(scope="session")
def scd_semantic_manifest_lookup(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestName, MetricFlowEngineTestFixture]
) -> SemanticManifestLookup:
    """Initialize semantic model for SCD tests."""
    return mf_engine_test_fixture_mapping[SemanticManifestName.SCD_MANIFEST].semantic_manifest_lookup


@pytest.fixture(scope="session")
def data_warehouse_validation_model(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestName, MetricFlowEngineTestFixture]
) -> SemanticManifest:
    """Model used for data warehouse validation tests."""
    return mf_engine_test_fixture_mapping[SemanticManifestName.DATA_WAREHOUSE_VALIDATION_MANIFEST].semantic_manifest


@pytest.fixture(scope="session")
def cyclic_join_semantic_manifest_lookup(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestName, MetricFlowEngineTestFixture]
) -> SemanticManifestLookup:
    """Manifest that contains a potential cycle in the join graph (if not handled properly)."""
    return mf_engine_test_fixture_mapping[SemanticManifestName.CYCLIC_JOIN_MANIFEST].semantic_manifest_lookup


@pytest.fixture(scope="session")
def ambiguous_resolution_manifest(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestName, MetricFlowEngineTestFixture]
) -> PydanticSemanticManifest:
    """Manifest used to test ambiguous resolution of group-by-items."""
    return mf_engine_test_fixture_mapping[SemanticManifestName.AMBIGUOUS_RESOLUTION_MANIFEST].semantic_manifest


@pytest.fixture(scope="session")
def ambiguous_resolution_manifest_lookup(  # noqa: D
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestName, MetricFlowEngineTestFixture]
) -> SemanticManifestLookup:
    return mf_engine_test_fixture_mapping[SemanticManifestName.AMBIGUOUS_RESOLUTION_MANIFEST].semantic_manifest_lookup


@pytest.fixture(scope="session")
def simple_multi_hop_join_manifest(  # noqa: D
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestName, MetricFlowEngineTestFixture]
) -> PydanticSemanticManifest:
    return mf_engine_test_fixture_mapping[SemanticManifestName.SIMPLE_MULTI_HOP_JOIN_MANIFEST].semantic_manifest


@pytest.fixture(scope="session")
def simple_multi_hop_join_manifest_lookup(  # noqa: D
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestName, MetricFlowEngineTestFixture]
) -> SemanticManifestLookup:
    """Manifest used to test ambiguous resolution of group-by-items."""
    return mf_engine_test_fixture_mapping[SemanticManifestName.SIMPLE_MULTI_HOP_JOIN_MANIFEST].semantic_manifest_lookup
