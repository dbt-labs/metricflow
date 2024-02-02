from __future__ import annotations

import logging
from collections import OrderedDict
from dataclasses import dataclass
from typing import Dict, List, Sequence

import pytest
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.parsing.dir_to_model import (
    parse_yaml_files_to_validation_ready_semantic_manifest,
)
from dbt_semantic_interfaces.parsing.objects import YamlConfigFile
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.transformations.pydantic_rule_set import PydanticSemanticManifestTransformRuleSet
from dbt_semantic_interfaces.transformations.semantic_manifest_transformer import PydanticSemanticManifestTransformer
from dbt_semantic_interfaces.validations.semantic_manifest_validator import SemanticManifestValidator

from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.dataflow_plan import BaseOutput, MetricTimeDimensionTransformNode, ReadSqlSourceNode
from metricflow.dataset.semantic_model_adapter import SemanticModelDataSet
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.column_resolver import DunderColumnAssociationResolver
from metricflow.query.query_parser import MetricFlowQueryParser
from metricflow.test.fixtures.id_fixtures import IdNumberSpace, patch_id_generators_helper

logger = logging.getLogger(__name__)


def query_parser_from_yaml(yaml_contents: List[YamlConfigFile]) -> MetricFlowQueryParser:
    """Given yaml files, return a query parser using default source nodes, resolvers and time spine source."""
    semantic_manifest_lookup = SemanticManifestLookup(
        parse_yaml_files_to_validation_ready_semantic_manifest(
            yaml_contents, apply_transformations=True
        ).semantic_manifest
    )
    SemanticManifestValidator[SemanticManifest]().checked_validations(semantic_manifest_lookup.semantic_manifest)
    return MetricFlowQueryParser(
        semantic_manifest_lookup=semantic_manifest_lookup,
    )


@dataclass(frozen=True)
class ConsistentIdObjectRepository:
    """Stores all objects that should have consistent IDs in tests."""

    simple_model_data_sets: OrderedDict[str, SemanticModelDataSet]
    simple_model_read_nodes: OrderedDict[str, ReadSqlSourceNode]
    simple_model_source_nodes: Sequence[BaseOutput]
    simple_model_time_spine_source_node: MetricTimeDimensionTransformNode

    multihop_model_read_nodes: OrderedDict[str, ReadSqlSourceNode]
    multihop_model_source_nodes: Sequence[BaseOutput]
    multihop_model_time_spine_source_node: MetricTimeDimensionTransformNode

    scd_model_data_sets: OrderedDict[str, SemanticModelDataSet]
    scd_model_read_nodes: OrderedDict[str, ReadSqlSourceNode]
    scd_model_source_nodes: Sequence[BaseOutput]
    scd_model_time_spine_source_node: MetricTimeDimensionTransformNode

    cyclic_join_read_nodes: OrderedDict[str, ReadSqlSourceNode]
    cyclic_join_source_nodes: Sequence[BaseOutput]
    cyclic_join_time_spine_source_node: MetricTimeDimensionTransformNode

    extended_date_model_read_nodes: OrderedDict[str, ReadSqlSourceNode]
    extended_date_model_source_nodes: Sequence[BaseOutput]
    extended_date_model_time_spine_source_node: MetricTimeDimensionTransformNode

    ambiguous_resolution_read_nodes: OrderedDict[str, ReadSqlSourceNode]
    ambiguous_resolution_source_nodes: Sequence[BaseOutput]
    ambiguous_resolution_time_spine_source_node: MetricTimeDimensionTransformNode


@pytest.fixture(scope="session")
def consistent_id_object_repository(
    simple_semantic_manifest_lookup: SemanticManifestLookup,
    partitioned_multi_hop_join_semantic_manifest_lookup: SemanticManifestLookup,
    scd_semantic_manifest_lookup: SemanticManifestLookup,
    cyclic_join_semantic_manifest_lookup: SemanticManifestLookup,
    extended_date_semantic_manifest_lookup: SemanticManifestLookup,
    ambiguous_resolution_manifest_lookup: SemanticManifestLookup,
) -> ConsistentIdObjectRepository:  # noqa: D
    """Create objects that have incremental numeric IDs with a consistent value.

    This should use IDs with a high enough value so that when other tests run with ID generators set to 0 at the start
    of the test and create objects, there is no overlap in the IDs.
    """
    with patch_id_generators_helper(start_value=IdNumberSpace.CONSISTENT_ID_REPOSITORY):
        sm_data_sets = create_data_sets(simple_semantic_manifest_lookup)
        multihop_data_sets = create_data_sets(partitioned_multi_hop_join_semantic_manifest_lookup)
        scd_data_sets = create_data_sets(scd_semantic_manifest_lookup)
        cyclic_join_data_sets = create_data_sets(cyclic_join_semantic_manifest_lookup)
        extended_date_data_sets = create_data_sets(extended_date_semantic_manifest_lookup)
        ambiguous_resolution_data_sets = create_data_sets(ambiguous_resolution_manifest_lookup)

        return ConsistentIdObjectRepository(
            simple_model_data_sets=sm_data_sets,
            simple_model_read_nodes=_data_set_to_read_nodes(sm_data_sets),
            simple_model_source_nodes=_data_set_to_source_nodes(simple_semantic_manifest_lookup, sm_data_sets),
            simple_model_time_spine_source_node=_build_time_spine_source_node(simple_semantic_manifest_lookup),
            multihop_model_read_nodes=_data_set_to_read_nodes(multihop_data_sets),
            multihop_model_source_nodes=_data_set_to_source_nodes(
                partitioned_multi_hop_join_semantic_manifest_lookup, multihop_data_sets
            ),
            multihop_model_time_spine_source_node=_build_time_spine_source_node(
                partitioned_multi_hop_join_semantic_manifest_lookup
            ),
            scd_model_data_sets=scd_data_sets,
            scd_model_read_nodes=_data_set_to_read_nodes(scd_data_sets),
            scd_model_source_nodes=_data_set_to_source_nodes(
                semantic_manifest_lookup=scd_semantic_manifest_lookup, data_sets=scd_data_sets
            ),
            scd_model_time_spine_source_node=_build_time_spine_source_node(scd_semantic_manifest_lookup),
            cyclic_join_read_nodes=_data_set_to_read_nodes(cyclic_join_data_sets),
            cyclic_join_source_nodes=_data_set_to_source_nodes(
                semantic_manifest_lookup=cyclic_join_semantic_manifest_lookup, data_sets=cyclic_join_data_sets
            ),
            cyclic_join_time_spine_source_node=_build_time_spine_source_node(cyclic_join_semantic_manifest_lookup),
            extended_date_model_read_nodes=_data_set_to_read_nodes(extended_date_data_sets),
            extended_date_model_source_nodes=_data_set_to_source_nodes(
                semantic_manifest_lookup=extended_date_semantic_manifest_lookup, data_sets=extended_date_data_sets
            ),
            extended_date_model_time_spine_source_node=_build_time_spine_source_node(
                extended_date_semantic_manifest_lookup
            ),
            ambiguous_resolution_read_nodes=_data_set_to_read_nodes(ambiguous_resolution_data_sets),
            ambiguous_resolution_source_nodes=_data_set_to_source_nodes(
                semantic_manifest_lookup=ambiguous_resolution_manifest_lookup, data_sets=ambiguous_resolution_data_sets
            ),
            ambiguous_resolution_time_spine_source_node=_build_time_spine_source_node(
                ambiguous_resolution_manifest_lookup
            ),
        )


@pytest.fixture(scope="session")
def simple_semantic_manifest_lookup_non_ds(template_mapping: Dict[str, str]) -> SemanticManifestLookup:  # noqa: D
    build_result = load_semantic_manifest("non_sm_manifest", template_mapping)
    return SemanticManifestLookup(build_result.semantic_manifest)


@pytest.fixture(scope="session")
def simple_semantic_manifest_lookup(template_mapping: Dict[str, str]) -> SemanticManifestLookup:  # noqa: D
    build_result = load_semantic_manifest("simple_manifest", template_mapping)
    return SemanticManifestLookup(build_result.semantic_manifest)


@pytest.fixture(scope="session")
def partitioned_multi_hop_join_semantic_manifest_lookup(  # noqa: D
    template_mapping: Dict[str, str]
) -> SemanticManifestLookup:
    build_result = load_semantic_manifest("partitioned_multi_hop_join_manifest", template_mapping)
    return SemanticManifestLookup(build_result.semantic_manifest)


@pytest.fixture(scope="session")
def multi_hop_join_semantic_manifest_lookup(template_mapping: Dict[str, str]) -> SemanticManifestLookup:  # noqa: D
    build_result = load_semantic_manifest("multi_hop_join_manifest", template_mapping)
    return SemanticManifestLookup(build_result.semantic_manifest)


@pytest.fixture(scope="session")
def simple_semantic_manifest(template_mapping: Dict[str, str]) -> SemanticManifest:
    """Model used for many tests."""
    build_result = load_semantic_manifest("simple_manifest", template_mapping)
    return build_result.semantic_manifest


@pytest.fixture(scope="session")
def simple_model__with_primary_transforms(template_mapping: Dict[str, str]) -> SemanticManifest:
    """Model used for tests pre-transformations."""
    build_result = load_semantic_manifest("simple_manifest", template_mapping)
    transformed_model = PydanticSemanticManifestTransformer().transform(
        model=build_result.semantic_manifest,
        ordered_rule_sequences=(PydanticSemanticManifestTransformRuleSet().primary_rules,),
    )
    return transformed_model


@pytest.fixture(scope="session")
def extended_date_semantic_manifest_lookup(template_mapping: Dict[str, str]) -> SemanticManifestLookup:  # noqa: D
    build_result = load_semantic_manifest("extended_date_manifest", template_mapping)
    return SemanticManifestLookup(build_result.semantic_manifest)


@pytest.fixture(scope="session")
def scd_semantic_manifest_lookup(template_mapping: Dict[str, str]) -> SemanticManifestLookup:
    """Initialize semantic model for SCD tests."""
    build_result = load_semantic_manifest("scd_manifest", template_mapping)
    return SemanticManifestLookup(build_result.semantic_manifest)


@pytest.fixture(scope="session")
def data_warehouse_validation_model(template_mapping: Dict[str, str]) -> SemanticManifest:
    """Model used for data warehouse validation tests."""
    build_result = load_semantic_manifest("data_warehouse_validation_manifest", template_mapping)
    return build_result.semantic_manifest


@pytest.fixture(scope="session")
def cyclic_join_semantic_manifest_lookup(template_mapping: Dict[str, str]) -> SemanticManifestLookup:
    """Manifest that contains a potential cycle in the join graph (if not handled properly)."""
    build_result = load_semantic_manifest("cyclic_join_manifest", template_mapping)
    return SemanticManifestLookup(build_result.semantic_manifest)


@pytest.fixture(scope="session")
def node_output_resolver(  # noqa:D
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> DataflowPlanNodeOutputDataSetResolver:
    return DataflowPlanNodeOutputDataSetResolver(
        column_association_resolver=DunderColumnAssociationResolver(simple_semantic_manifest_lookup),
        semantic_manifest_lookup=simple_semantic_manifest_lookup,
    )


@pytest.fixture(scope="session")
def ambiguous_resolution_manifest(template_mapping: Dict[str, str]) -> PydanticSemanticManifest:
    """Manifest used to test ambiguous resolution of group-by-items."""
    build_result = load_semantic_manifest("ambiguous_resolution_manifest", template_mapping)
    return build_result.semantic_manifest


@pytest.fixture(scope="session")
def ambiguous_resolution_manifest_lookup(  # noqa: D
    ambiguous_resolution_manifest: PydanticSemanticManifest,
) -> SemanticManifestLookup:
    return SemanticManifestLookup(ambiguous_resolution_manifest)


@pytest.fixture(scope="session")
def simple_multi_hop_join_manifest(template_mapping: Dict[str, str]) -> PydanticSemanticManifest:  # noqa: D
    build_result = load_semantic_manifest("simple_multi_hop_join_manifest", template_mapping)
    return build_result.semantic_manifest


@pytest.fixture(scope="session")
def simple_multi_hop_join_manifest_lookup(  # noqa: D
    simple_multi_hop_join_manifest: PydanticSemanticManifest,
) -> SemanticManifestLookup:
    """Manifest used to test ambiguous resolution of group-by-items."""
    return SemanticManifestLookup(simple_multi_hop_join_manifest)
