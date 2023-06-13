from __future__ import annotations

import logging
import os
from collections import OrderedDict
from dataclasses import dataclass
from typing import Dict, List, Sequence

import pytest
from dbt_semantic_interfaces.parsing.dir_to_model import (
    parse_directory_of_yaml_files_to_semantic_manifest,
    parse_yaml_files_to_validation_ready_semantic_manifest,
)
from dbt_semantic_interfaces.parsing.objects import YamlConfigFile
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.protocols.semantic_model import SemanticModel
from dbt_semantic_interfaces.transformations.pydantic_rule_set import PydanticSemanticManifestTransformRuleSet
from dbt_semantic_interfaces.transformations.semantic_manifest_transformer import PydanticSemanticManifestTransformer
from dbt_semantic_interfaces.validations.semantic_manifest_validator import SemanticManifestValidator

from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.builder.source_node import SourceNodeBuilder
from metricflow.dataflow.dataflow_plan import BaseOutput, ReadSqlSourceNode
from metricflow.dataset.convert_semantic_model import SemanticModelToDataSetConverter
from metricflow.dataset.semantic_model_adapter import SemanticModelDataSet
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.column_resolver import DunderColumnAssociationResolver
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.query.query_parser import MetricFlowQueryParser
from metricflow.test.fixtures.id_fixtures import IdNumberSpace, patch_id_generators_helper
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState

logger = logging.getLogger(__name__)


def _data_set_to_read_nodes(
    data_sets: OrderedDict[str, SemanticModelDataSet]
) -> OrderedDict[str, ReadSqlSourceNode[SemanticModelDataSet]]:
    """Return a mapping from the name of the semantic model to the dataflow plan node that reads from it."""
    return_dict: OrderedDict[str, ReadSqlSourceNode[SemanticModelDataSet]] = OrderedDict()
    for semantic_model_name, data_set in data_sets.items():
        return_dict[semantic_model_name] = ReadSqlSourceNode[SemanticModelDataSet](data_set)
        logger.debug(
            f"For semantic model {semantic_model_name}, creating node_id {return_dict[semantic_model_name].node_id}"
        )

    return return_dict


def _data_set_to_source_nodes(
    semantic_manifest_lookup: SemanticManifestLookup, data_sets: OrderedDict[str, SemanticModelDataSet]
) -> Sequence[BaseOutput[SemanticModelDataSet]]:
    source_node_builder = SourceNodeBuilder(semantic_manifest_lookup)
    return source_node_builder.create_from_data_sets(list(data_sets.values()))


def query_parser_from_yaml(
    yaml_contents: List[YamlConfigFile], time_spine_source: TimeSpineSource
) -> MetricFlowQueryParser:
    """Given yaml files, return a query parser using default source nodes, resolvers and time spine source."""
    semantic_manifest_lookup = SemanticManifestLookup(
        parse_yaml_files_to_validation_ready_semantic_manifest(
            yaml_contents, apply_transformations=True
        ).semantic_manifest
    )
    SemanticManifestValidator[SemanticManifest]().checked_validations(semantic_manifest_lookup.semantic_manifest)
    source_nodes = _data_set_to_source_nodes(semantic_manifest_lookup, create_data_sets(semantic_manifest_lookup))
    return MetricFlowQueryParser(
        model=semantic_manifest_lookup,
        column_association_resolver=DunderColumnAssociationResolver(semantic_manifest_lookup),
        source_nodes=source_nodes,
        node_output_resolver=DataflowPlanNodeOutputDataSetResolver(
            column_association_resolver=DunderColumnAssociationResolver(semantic_manifest_lookup),
            semantic_manifest_lookup=semantic_manifest_lookup,
            time_spine_source=time_spine_source,
        ),
    )


@dataclass(frozen=True)
class ConsistentIdObjectRepository:
    """Stores all objects that should have consistent IDs in tests."""

    simple_model_data_sets: OrderedDict[str, SemanticModelDataSet]
    simple_model_read_nodes: OrderedDict[str, ReadSqlSourceNode[SemanticModelDataSet]]
    simple_model_source_nodes: Sequence[BaseOutput[SemanticModelDataSet]]

    multihop_model_read_nodes: OrderedDict[str, ReadSqlSourceNode[SemanticModelDataSet]]
    multihop_model_source_nodes: Sequence[BaseOutput[SemanticModelDataSet]]

    scd_model_data_sets: OrderedDict[str, SemanticModelDataSet]
    scd_model_read_nodes: OrderedDict[str, ReadSqlSourceNode[SemanticModelDataSet]]
    scd_model_source_nodes: Sequence[BaseOutput[SemanticModelDataSet]]


@pytest.fixture(scope="session")
def consistent_id_object_repository(
    simple_semantic_manifest_lookup: SemanticManifestLookup,
    multi_hop_join_semantic_manifest_lookup: SemanticManifestLookup,
    scd_semantic_manifest_lookup: SemanticManifestLookup,
) -> ConsistentIdObjectRepository:  # noqa: D
    """Create objects that have incremental numeric IDs with a consistent value.

    This should use IDs with a high enough value so that when other tests run with ID generators set to 0 at the start
    of the test and create objects, there is no overlap in the IDs.
    """
    with patch_id_generators_helper(start_value=IdNumberSpace.CONSISTENT_ID_REPOSITORY):
        sm_data_sets = create_data_sets(simple_semantic_manifest_lookup)
        multihop_data_sets = create_data_sets(multi_hop_join_semantic_manifest_lookup)
        scd_data_sets = create_data_sets(scd_semantic_manifest_lookup)

        return ConsistentIdObjectRepository(
            simple_model_data_sets=sm_data_sets,
            simple_model_read_nodes=_data_set_to_read_nodes(sm_data_sets),
            simple_model_source_nodes=_data_set_to_source_nodes(simple_semantic_manifest_lookup, sm_data_sets),
            multihop_model_read_nodes=_data_set_to_read_nodes(multihop_data_sets),
            multihop_model_source_nodes=_data_set_to_source_nodes(
                multi_hop_join_semantic_manifest_lookup, multihop_data_sets
            ),
            scd_model_data_sets=scd_data_sets,
            scd_model_read_nodes=_data_set_to_read_nodes(scd_data_sets),
            scd_model_source_nodes=_data_set_to_source_nodes(
                semantic_manifest_lookup=scd_semantic_manifest_lookup, data_sets=scd_data_sets
            ),
        )


def create_data_sets(
    multihop_semantic_manifest_lookup: SemanticManifestLookup,
) -> OrderedDict[str, SemanticModelDataSet]:
    """Convert the SemanticModels in the model to SqlDataSets.

    Key is the name of the semantic model, value is the associated data set.
    """
    # Use ordered dict and sort by name to get consistency when running tests.
    data_sets = OrderedDict()
    semantic_models: Sequence[SemanticModel] = multihop_semantic_manifest_lookup.semantic_manifest.semantic_models
    semantic_models = sorted(semantic_models, key=lambda x: x.name)

    converter = SemanticModelToDataSetConverter(
        column_association_resolver=DunderColumnAssociationResolver(multihop_semantic_manifest_lookup)
    )

    for semantic_model in semantic_models:
        data_sets[semantic_model.name] = converter.create_sql_source_data_set(semantic_model)

    return data_sets


@pytest.fixture(scope="session")
def template_mapping(mf_test_session_state: MetricFlowTestSessionState) -> Dict[str, str]:
    """Mapping for template variables in the model YAML files."""
    return {"source_schema": mf_test_session_state.mf_source_schema}


@pytest.fixture(scope="session")
def simple_semantic_manifest_lookup_non_ds(template_mapping: Dict[str, str]) -> SemanticManifestLookup:  # noqa: D
    build_result = parse_directory_of_yaml_files_to_semantic_manifest(
        os.path.join(os.path.dirname(__file__), "model_yamls/non_sm_model"), template_mapping=template_mapping
    )
    return SemanticManifestLookup(build_result.semantic_manifest)


@pytest.fixture(scope="session")
def simple_semantic_manifest_lookup(template_mapping: Dict[str, str]) -> SemanticManifestLookup:  # noqa: D
    build_result = parse_directory_of_yaml_files_to_semantic_manifest(
        os.path.join(os.path.dirname(__file__), "model_yamls/simple_model"), template_mapping=template_mapping
    )
    return SemanticManifestLookup(build_result.semantic_manifest)


@pytest.fixture(scope="session")
def multi_hop_join_semantic_manifest_lookup(template_mapping: Dict[str, str]) -> SemanticManifestLookup:  # noqa: D
    build_result = parse_directory_of_yaml_files_to_semantic_manifest(
        os.path.join(os.path.dirname(__file__), "model_yamls/multi_hop_join_model/partitioned_semantic_models"),
        template_mapping=template_mapping,
    )
    return SemanticManifestLookup(build_result.semantic_manifest)


@pytest.fixture(scope="session")
def unpartitioned_multi_hop_join_semantic_manifest_lookup(  # noqa: D
    template_mapping: Dict[str, str]
) -> SemanticManifestLookup:
    build_result = parse_directory_of_yaml_files_to_semantic_manifest(
        os.path.join(os.path.dirname(__file__), "model_yamls/multi_hop_join_model/unpartitioned_semantic_models"),
        template_mapping=template_mapping,
    )
    return SemanticManifestLookup(build_result.semantic_manifest)


@pytest.fixture(scope="session")
def simple_semantic_manifest(template_mapping: Dict[str, str]) -> SemanticManifest:
    """Model used for many tests."""
    build_result = parse_directory_of_yaml_files_to_semantic_manifest(
        os.path.join(os.path.dirname(__file__), "model_yamls/simple_model"), template_mapping=template_mapping
    )
    return build_result.semantic_manifest


@pytest.fixture(scope="session")
def simple_model__with_primary_transforms(template_mapping: Dict[str, str]) -> SemanticManifest:
    """Model used for tests pre-transformations."""
    build_result = parse_directory_of_yaml_files_to_semantic_manifest(
        os.path.join(os.path.dirname(__file__), "model_yamls/simple_model"),
        template_mapping=template_mapping,
        apply_transformations=False,
    )
    transformed_model = PydanticSemanticManifestTransformer().transform(
        model=build_result.semantic_manifest,
        ordered_rule_sequences=(PydanticSemanticManifestTransformRuleSet().primary_rules,),
    )
    return transformed_model


@pytest.fixture(scope="session")
def extended_date_semantic_manifest_lookup(template_mapping: Dict[str, str]) -> SemanticManifestLookup:  # noqa: D
    build_result = parse_directory_of_yaml_files_to_semantic_manifest(
        os.path.join(os.path.dirname(__file__), "model_yamls/extended_date_model"),
        template_mapping=template_mapping,
    )
    return SemanticManifestLookup(build_result.semantic_manifest)


@pytest.fixture(scope="session")
def scd_semantic_manifest_lookup(template_mapping: Dict[str, str]) -> SemanticManifestLookup:
    """Initialize semantic model for SCD tests."""
    build_result = parse_directory_of_yaml_files_to_semantic_manifest(
        os.path.join(os.path.dirname(__file__), "model_yamls/scd_model"), template_mapping=template_mapping
    )
    return SemanticManifestLookup(build_result.semantic_manifest)


@pytest.fixture(scope="session")
def data_warehouse_validation_model(template_mapping: Dict[str, str]) -> SemanticManifest:
    """Model used for data warehouse validation tests."""
    build_result = parse_directory_of_yaml_files_to_semantic_manifest(
        os.path.join(os.path.dirname(__file__), "model_yamls/data_warehouse_validation_model"),
        template_mapping=template_mapping,
    )
    return build_result.semantic_manifest
