from __future__ import annotations

import logging
from typing import Dict

import pytest
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.dunder_column_association_resolver import DunderColumnAssociationResolver
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.manifest_helpers import mf_load_manifest_from_yaml_directory
from metricflow_semantics.test_helpers.semantic_manifest_yamls.ambiguous_resolution_manifest import (
    AMBIGUOUS_RESOLUTION_MANIFEST_ANCHOR,
)
from metricflow_semantics.test_helpers.semantic_manifest_yamls.cyclic_join_manifest import CYCLIC_JOIN_MANIFEST_ANCHOR
from metricflow_semantics.test_helpers.semantic_manifest_yamls.extended_date_manifest import (
    EXTENDED_DATE_MANIFEST_ANCHOR,
)
from metricflow_semantics.test_helpers.semantic_manifest_yamls.multi_hop_join_manifest import (
    MULTI_HOP_JOIN_MANIFEST_ANCHOR,
)
from metricflow_semantics.test_helpers.semantic_manifest_yamls.partitioned_multi_hop_join_manifest import (
    PARTITIONED_MULTI_HOP_JOIN_MANIFEST_ANCHOR,
)
from metricflow_semantics.test_helpers.semantic_manifest_yamls.scd_manifest import SCD_MANIFEST_ANCHOR
from metricflow_semantics.test_helpers.semantic_manifest_yamls.sg_00_minimal_manifest import SG_00_MINIMAL_MANIFEST
from metricflow_semantics.test_helpers.semantic_manifest_yamls.sg_02_single_join import SG_02_SINGLE_JOIN
from metricflow_semantics.test_helpers.semantic_manifest_yamls.sg_05_derived_metric import SG_05_DERIVED_METRIC_MANIFEST
from metricflow_semantics.test_helpers.semantic_manifest_yamls.simple_manifest import SIMPLE_MANIFEST_ANCHOR
from metricflow_semantics.test_helpers.semantic_manifest_yamls.simple_multi_hop_join_manifest import (
    SIMPLE_MULTI_HOP_JOIN_MANIFEST_ANCHOR,
)

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def template_mapping(mf_test_configuration: MetricFlowTestConfiguration) -> Dict[str, str]:
    """Mapping for template variables in the model YAML files."""
    return {"source_schema": mf_test_configuration.mf_source_schema}


@pytest.fixture(scope="session")
def simple_semantic_manifest(template_mapping: Dict[str, str]) -> PydanticSemanticManifest:
    """Manifest used for many tests."""
    return mf_load_manifest_from_yaml_directory(SIMPLE_MANIFEST_ANCHOR.directory, template_mapping)


@pytest.fixture(scope="session")
def simple_semantic_manifest_lookup(  # noqa: D103
    simple_semantic_manifest: PydanticSemanticManifest,
) -> SemanticManifestLookup:
    return SemanticManifestLookup(simple_semantic_manifest)


@pytest.fixture(scope="session")
def multi_hop_join_manifest(template_mapping: Dict[str, str]) -> PydanticSemanticManifest:
    """Manifest used for many tests."""
    return mf_load_manifest_from_yaml_directory(MULTI_HOP_JOIN_MANIFEST_ANCHOR.directory, template_mapping)


@pytest.fixture(scope="session")
def multi_hop_join_manifest_lookup(  # noqa: D103
    multi_hop_join_manifest: PydanticSemanticManifest,
) -> SemanticManifestLookup:
    return SemanticManifestLookup(multi_hop_join_manifest)


@pytest.fixture(scope="session")
def simple_multi_hop_join_manifest(template_mapping: Dict[str, str]) -> PydanticSemanticManifest:
    """Manifest used for many tests."""
    return mf_load_manifest_from_yaml_directory(SIMPLE_MULTI_HOP_JOIN_MANIFEST_ANCHOR.directory, template_mapping)


@pytest.fixture(scope="session")
def extended_date_manifest(template_mapping: Dict[str, str]) -> PydanticSemanticManifest:
    """Manifest used for many tests."""
    return mf_load_manifest_from_yaml_directory(EXTENDED_DATE_MANIFEST_ANCHOR.directory, template_mapping)


@pytest.fixture(scope="session")
def simple_multi_hop_join_manifest_lookup(  # noqa: D103
    simple_multi_hop_join_manifest: PydanticSemanticManifest,
) -> SemanticManifestLookup:
    return SemanticManifestLookup(simple_multi_hop_join_manifest)


@pytest.fixture(scope="session")
def partitioned_multi_hop_join_semantic_manifest(  # noqa: D103
    template_mapping: Dict[str, str]
) -> PydanticSemanticManifest:
    return mf_load_manifest_from_yaml_directory(PARTITIONED_MULTI_HOP_JOIN_MANIFEST_ANCHOR.directory, template_mapping)


@pytest.fixture(scope="session")
def partitioned_multi_hop_join_semantic_manifest_lookup(  # noqa: D103
    partitioned_multi_hop_join_semantic_manifest: PydanticSemanticManifest,
) -> SemanticManifestLookup:
    return SemanticManifestLookup(partitioned_multi_hop_join_semantic_manifest)


@pytest.fixture(scope="session")
def scd_semantic_manifest(template_mapping: Dict[str, str]) -> PydanticSemanticManifest:  # noqa: D103
    return mf_load_manifest_from_yaml_directory(SCD_MANIFEST_ANCHOR.directory, template_mapping)


@pytest.fixture(scope="session")
def scd_semantic_manifest_lookup(  # noqa: D103
    scd_semantic_manifest: PydanticSemanticManifest,
) -> SemanticManifestLookup:
    return SemanticManifestLookup(scd_semantic_manifest)


@pytest.fixture(scope="session")
def ambiguous_resolution_manifest(template_mapping: Dict[str, str]) -> PydanticSemanticManifest:  # noqa: D103
    return mf_load_manifest_from_yaml_directory(AMBIGUOUS_RESOLUTION_MANIFEST_ANCHOR.directory, template_mapping)


@pytest.fixture(scope="session")
def ambiguous_resolution_manifest_lookup(  # noqa: D103
    ambiguous_resolution_manifest: PydanticSemanticManifest,
) -> SemanticManifestLookup:
    return SemanticManifestLookup(ambiguous_resolution_manifest)


@pytest.fixture(scope="session")
def cyclic_join_manifest(template_mapping: Dict[str, str]) -> PydanticSemanticManifest:  # noqa: D103
    return mf_load_manifest_from_yaml_directory(CYCLIC_JOIN_MANIFEST_ANCHOR.directory, template_mapping)


@pytest.fixture(scope="session")
def cyclic_join_semantic_manifest_lookup(  # noqa: D103
    cyclic_join_manifest: PydanticSemanticManifest,
) -> SemanticManifestLookup:
    return SemanticManifestLookup(cyclic_join_manifest)


@pytest.fixture(scope="session")
def column_association_resolver(  # noqa: D103
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> ColumnAssociationResolver:
    return DunderColumnAssociationResolver()


@pytest.fixture(scope="session")
def extended_date_semantic_manifest_lookup(  # noqa: D103
    extended_date_manifest: PydanticSemanticManifest,
) -> SemanticManifestLookup:
    return SemanticManifestLookup(extended_date_manifest)


@pytest.fixture(scope="session")
def sg_00_minimal_manifest(template_mapping: Dict[str, str]) -> PydanticSemanticManifest:  # noqa: D103
    return mf_load_manifest_from_yaml_directory(SG_00_MINIMAL_MANIFEST.directory, template_mapping)


@pytest.fixture(scope="session")
def sg_02_single_join_manifest(template_mapping: Dict[str, str]) -> PydanticSemanticManifest:  # noqa: D103
    return mf_load_manifest_from_yaml_directory(SG_02_SINGLE_JOIN.directory, template_mapping)


@pytest.fixture(scope="session")
def sg_05_derived_metric_manifest(template_mapping: Dict[str, str]) -> PydanticSemanticManifest:  # noqa: D103
    return mf_load_manifest_from_yaml_directory(SG_05_DERIVED_METRIC_MANIFEST.directory, template_mapping)
