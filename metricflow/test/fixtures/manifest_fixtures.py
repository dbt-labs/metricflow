from __future__ import annotations

import logging
import os
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Mapping, Optional, Sequence

import pytest
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.parsing.dir_to_model import (
    SemanticManifestBuildResult,
    parse_directory_of_yaml_files_to_semantic_manifest,
)
from dbt_semantic_interfaces.protocols import SemanticModel
from dbt_semantic_interfaces.test_utils import as_datetime
from dbt_semantic_interfaces.validations.semantic_manifest_validator import SemanticManifestValidator

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.builder.source_node import SourceNodeBuilder, SourceNodeSet
from metricflow.dataflow.dataflow_plan import ReadSqlSourceNode
from metricflow.dataset.convert_semantic_model import SemanticModelToDataSetConverter
from metricflow.dataset.semantic_model_adapter import SemanticModelDataSet
from metricflow.engine.metricflow_engine import MetricFlowEngine
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.column_resolver import DunderColumnAssociationResolver
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.query.query_parser import MetricFlowQueryParser
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.test.fixtures.id_fixtures import IdNumberSpace, patch_id_generators_helper
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.time.configurable_time_source import ConfigurableTimeSource

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SemanticManifestSetupPropertySet:
    """Describes a semantic manifest used in testing."""

    # Name corresponds to a directory under test/fixtures/semantic_manifest_yamls.
    semantic_manifest_name: str
    # Specify a separate start value for each semantic manifest to reduce snapshot thrash when one semantic manifest
    # is modified. i.e. without this, modifying the first semantic manifest might cause all IDs in snapshots associated
    # with semantic manifests following the first one to change.
    id_number_space: IdNumberSpace


class SemanticManifestSetup(Enum):
    """Enumeration of semantic manifests that defined in YAML files and used for testing."""

    AMBIGUOUS_RESOLUTION_MANIFEST = SemanticManifestSetupPropertySet(
        semantic_manifest_name="ambiguous_resolution_manifest",
        id_number_space=IdNumberSpace.for_block(0),
    )
    # Not including CONFIG_LINTER_MANIFEST as it has intentional errors for running validations.
    CYCLIC_JOIN_MANIFEST = SemanticManifestSetupPropertySet(
        semantic_manifest_name="cyclic_join_manifest", id_number_space=IdNumberSpace.for_block(1)
    )
    DATA_WAREHOUSE_VALIDATION_MANIFEST = SemanticManifestSetupPropertySet(
        semantic_manifest_name="data_warehouse_validation_manifest", id_number_space=IdNumberSpace.for_block(2)
    )
    EXTENDED_DATE_MANIFEST = SemanticManifestSetupPropertySet(
        semantic_manifest_name="extended_date_manifest", id_number_space=IdNumberSpace.for_block(3)
    )
    JOIN_TYPES_MANIFEST = SemanticManifestSetupPropertySet(
        semantic_manifest_name="join_types_manifest", id_number_space=IdNumberSpace.for_block(4)
    )
    MULTI_HOP_JOIN_MANIFEST = SemanticManifestSetupPropertySet(
        semantic_manifest_name="multi_hop_join_manifest", id_number_space=IdNumberSpace.for_block(5)
    )
    PARTITIONED_MULTI_HOP_JOIN_MANIFEST = SemanticManifestSetupPropertySet(
        semantic_manifest_name="partitioned_multi_hop_join_manifest", id_number_space=IdNumberSpace.for_block(6)
    )
    NON_SM_MANIFEST = SemanticManifestSetupPropertySet(
        semantic_manifest_name="non_sm_manifest", id_number_space=IdNumberSpace.for_block(7)
    )
    SCD_MANIFEST = SemanticManifestSetupPropertySet(
        semantic_manifest_name="scd_manifest", id_number_space=IdNumberSpace.for_block(8)
    )
    SIMPLE_MANIFEST = SemanticManifestSetupPropertySet(
        semantic_manifest_name="simple_manifest", id_number_space=IdNumberSpace.for_block(9)
    )
    SIMPLE_MULTI_HOP_JOIN_MANIFEST = SemanticManifestSetupPropertySet(
        semantic_manifest_name="simple_multi_hop_join_manifest", id_number_space=IdNumberSpace.for_block(10)
    )

    @property
    def id_number_space(self) -> IdNumberSpace:  # noqa: D
        return self.value.id_number_space

    @property
    def semantic_manifest_name(self) -> str:  # noqa: D
        return self.value.semantic_manifest_name


@dataclass(frozen=True)
class MetricFlowEngineTestFixture:
    """Contains objects for testing the MF engine for a specific semantic manifest."""

    semantic_manifest: PydanticSemanticManifest
    semantic_manifest_lookup: SemanticManifestLookup
    column_association_resolver: ColumnAssociationResolver
    data_set_mapping: OrderedDict[str, SemanticModelDataSet]
    read_node_mapping: OrderedDict[str, ReadSqlSourceNode]
    source_node_set: SourceNodeSet
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter
    query_parser: MetricFlowQueryParser
    metricflow_engine: MetricFlowEngine

    _node_output_resolver: DataflowPlanNodeOutputDataSetResolver

    @staticmethod
    def from_parameters(  # noqa: D
        sql_client: SqlClient, semantic_manifest: PydanticSemanticManifest
    ) -> MetricFlowEngineTestFixture:
        semantic_manifest_lookup = SemanticManifestLookup(semantic_manifest)
        data_set_mapping = MetricFlowEngineTestFixture._create_data_sets(semantic_manifest_lookup)
        read_node_mapping = MetricFlowEngineTestFixture._data_set_to_read_nodes(data_set_mapping)
        column_association_resolver = DunderColumnAssociationResolver(semantic_manifest_lookup)
        source_node_set = MetricFlowEngineTestFixture._data_set_to_source_node_set(
            column_association_resolver, semantic_manifest_lookup, data_set_mapping
        )
        node_output_resolver = DataflowPlanNodeOutputDataSetResolver(
            column_association_resolver=column_association_resolver,
            semantic_manifest_lookup=semantic_manifest_lookup,
        )
        node_output_resolver.cache_output_data_sets(source_node_set.all_nodes)
        query_parser = MetricFlowQueryParser(semantic_manifest_lookup=semantic_manifest_lookup)
        return MetricFlowEngineTestFixture(
            semantic_manifest=semantic_manifest,
            semantic_manifest_lookup=semantic_manifest_lookup,
            column_association_resolver=column_association_resolver,
            data_set_mapping=data_set_mapping,
            read_node_mapping=read_node_mapping,
            source_node_set=source_node_set,
            _node_output_resolver=node_output_resolver,
            dataflow_to_sql_converter=DataflowToSqlQueryPlanConverter(
                column_association_resolver=column_association_resolver,
                semantic_manifest_lookup=semantic_manifest_lookup,
            ),
            query_parser=query_parser,
            metricflow_engine=MetricFlowEngine(
                semantic_manifest_lookup=semantic_manifest_lookup,
                sql_client=sql_client,
                time_source=ConfigurableTimeSource(as_datetime("2020-01-01")),
                query_parser=query_parser,
                column_association_resolver=column_association_resolver,
            ),
        )

    @property
    def dataflow_plan_builder(self) -> DataflowPlanBuilder:
        """Return a DataflowPlanBuilder that can be used for tests.

        This should be recreated for each test since DataflowPlanBuilder contains a stateful cache.
        """
        return DataflowPlanBuilder(
            source_node_set=self.source_node_set,
            semantic_manifest_lookup=self.semantic_manifest_lookup,
            node_output_resolver=self._node_output_resolver.copy(),
            column_association_resolver=self.column_association_resolver,
        )

    @staticmethod
    def _data_set_to_read_nodes(
        data_sets: OrderedDict[str, SemanticModelDataSet]
    ) -> OrderedDict[str, ReadSqlSourceNode]:
        """Return a mapping from the name of the semantic model to the dataflow plan node that reads from it."""
        # Moved from model_fixtures.py.
        return_dict: OrderedDict[str, ReadSqlSourceNode] = OrderedDict()
        for semantic_model_name, data_set in data_sets.items():
            return_dict[semantic_model_name] = ReadSqlSourceNode(data_set)
            logger.debug(
                f"For semantic model {semantic_model_name}, creating node_id {return_dict[semantic_model_name].node_id}"
            )

        return return_dict

    @staticmethod
    def _data_set_to_source_node_set(
        column_association_resolver: ColumnAssociationResolver,
        semantic_manifest_lookup: SemanticManifestLookup,
        data_sets: OrderedDict[str, SemanticModelDataSet],
    ) -> SourceNodeSet:
        # Moved from model_fixtures.py.
        source_node_builder = SourceNodeBuilder(column_association_resolver, semantic_manifest_lookup)
        return source_node_builder.create_from_data_sets(list(data_sets.values()))

    @staticmethod
    def _create_data_sets(
        multihop_semantic_manifest_lookup: SemanticManifestLookup,
    ) -> OrderedDict[str, SemanticModelDataSet]:
        """Convert the SemanticModels in the model to SqlDataSets.

        Key is the name of the semantic model, value is the associated data set.
        """
        # Moved from model_fixtures.py.

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
def mf_engine_test_fixture_mapping(
    template_mapping: Dict[str, str],
    sql_client: SqlClient,
) -> Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]:
    """Returns a mapping for all semantic manifests used in testing to the associated test fixture."""
    fixture_mapping: Dict[SemanticManifestSetup, MetricFlowEngineTestFixture] = {}
    for semantic_manifest_setup in SemanticManifestSetup:
        with patch_id_generators_helper(semantic_manifest_setup.id_number_space.start_value):
            try:
                build_result = load_semantic_manifest(semantic_manifest_setup.semantic_manifest_name, template_mapping)
            except Exception as e:
                raise RuntimeError(f"Error while loading semantic manifest: {semantic_manifest_setup}") from e

            fixture_mapping[semantic_manifest_setup] = MetricFlowEngineTestFixture.from_parameters(
                sql_client, build_result.semantic_manifest
            )

    return fixture_mapping


def load_semantic_manifest(
    relative_manifest_path: str,
    template_mapping: Optional[Dict[str, str]] = None,
) -> SemanticManifestBuildResult:
    """Reads the manifest YAMLs from the standard location, applies transformations, runs validations."""
    yaml_file_directory = os.path.join(os.path.dirname(__file__), f"semantic_manifest_yamls/{relative_manifest_path}")
    build_result = parse_directory_of_yaml_files_to_semantic_manifest(
        yaml_file_directory, template_mapping=template_mapping
    )
    validator = SemanticManifestValidator[PydanticSemanticManifest]()
    validator.checked_validations(build_result.semantic_manifest)
    return build_result


@pytest.fixture(scope="session")
def template_mapping(mf_test_session_state: MetricFlowTestSessionState) -> Dict[str, str]:
    """Mapping for template variables in the model YAML files."""
    return {"source_schema": mf_test_session_state.mf_source_schema}


@pytest.fixture(scope="session")
def simple_semantic_manifest_lookup_non_ds(  # noqa: D
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> SemanticManifestLookup:
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.NON_SM_MANIFEST].semantic_manifest_lookup


@pytest.fixture(scope="session")
def simple_semantic_manifest_lookup(  # noqa: D
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> SemanticManifestLookup:
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].semantic_manifest_lookup


@pytest.fixture(scope="session")
def partitioned_multi_hop_join_semantic_manifest_lookup(  # noqa: D
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> SemanticManifestLookup:
    return mf_engine_test_fixture_mapping[
        SemanticManifestSetup.PARTITIONED_MULTI_HOP_JOIN_MANIFEST
    ].semantic_manifest_lookup


@pytest.fixture(scope="session")
def multi_hop_join_semantic_manifest_lookup(  # noqa: D
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> SemanticManifestLookup:
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.MULTI_HOP_JOIN_MANIFEST].semantic_manifest_lookup


@pytest.fixture(scope="session")
def simple_semantic_manifest(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> PydanticSemanticManifest:
    """Model used for many tests."""
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].semantic_manifest


@pytest.fixture(scope="session")
def extended_date_semantic_manifest_lookup(  # noqa: D
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> SemanticManifestLookup:
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.EXTENDED_DATE_MANIFEST].semantic_manifest_lookup


@pytest.fixture(scope="session")
def scd_semantic_manifest_lookup(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> SemanticManifestLookup:
    """Initialize semantic model for SCD tests."""
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.SCD_MANIFEST].semantic_manifest_lookup


@pytest.fixture(scope="session")
def data_warehouse_validation_model(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> PydanticSemanticManifest:
    """Model used for data warehouse validation tests."""
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.DATA_WAREHOUSE_VALIDATION_MANIFEST].semantic_manifest


@pytest.fixture(scope="session")
def cyclic_join_semantic_manifest_lookup(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> SemanticManifestLookup:
    """Manifest that contains a potential cycle in the join graph (if not handled properly)."""
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.CYCLIC_JOIN_MANIFEST].semantic_manifest_lookup


@pytest.fixture(scope="session")
def ambiguous_resolution_manifest(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> PydanticSemanticManifest:
    """Manifest used to test ambiguous resolution of group-by-items."""
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.AMBIGUOUS_RESOLUTION_MANIFEST].semantic_manifest


@pytest.fixture(scope="session")
def ambiguous_resolution_manifest_lookup(  # noqa: D
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> SemanticManifestLookup:
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.AMBIGUOUS_RESOLUTION_MANIFEST].semantic_manifest_lookup


@pytest.fixture(scope="session")
def simple_multi_hop_join_manifest(  # noqa: D
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> PydanticSemanticManifest:
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MULTI_HOP_JOIN_MANIFEST].semantic_manifest


@pytest.fixture(scope="session")
def simple_multi_hop_join_manifest_lookup(  # noqa: D
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> SemanticManifestLookup:
    """Manifest used to test ambiguous resolution of group-by-items."""
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MULTI_HOP_JOIN_MANIFEST].semantic_manifest_lookup
