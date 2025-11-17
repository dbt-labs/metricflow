from __future__ import annotations

import logging
import pathlib
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Mapping

import pytest
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.test_utils import as_datetime
from metricflow_semantics.dag.sequential_id import SequentialIdGenerator
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.dunder_column_association_resolver import DunderColumnAssociationResolver
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.id_helpers import IdNumberSpace
from metricflow_semantics.test_helpers.manifest_helpers import mf_load_manifest_from_yaml_directory
from metricflow_semantics.test_helpers.semantic_manifest_yamls.ambiguous_resolution_manifest import (
    AMBIGUOUS_RESOLUTION_MANIFEST_ANCHOR,
)
from metricflow_semantics.test_helpers.semantic_manifest_yamls.cyclic_join_manifest import CYCLIC_JOIN_MANIFEST_ANCHOR
from metricflow_semantics.test_helpers.semantic_manifest_yamls.data_warehouse_validation_manifest import (
    DW_VALIDATION_MANIFEST_ANCHOR,
)
from metricflow_semantics.test_helpers.semantic_manifest_yamls.extended_date_manifest import (
    EXTENDED_DATE_MANIFEST_ANCHOR,
)
from metricflow_semantics.test_helpers.semantic_manifest_yamls.join_types_manifest import JOIN_TYPES_MANIFEST_ANCHOR
from metricflow_semantics.test_helpers.semantic_manifest_yamls.multi_hop_join_manifest import (
    MULTI_HOP_JOIN_MANIFEST_ANCHOR,
)
from metricflow_semantics.test_helpers.semantic_manifest_yamls.name_edge_case_manifest import (
    NAME_EDGE_CASE_MANIFEST_ANCHOR,
)
from metricflow_semantics.test_helpers.semantic_manifest_yamls.non_sm_manifest import NON_SM_MANIFEST_ANCHOR
from metricflow_semantics.test_helpers.semantic_manifest_yamls.partitioned_multi_hop_join_manifest import (
    PARTITIONED_MULTI_HOP_JOIN_MANIFEST_ANCHOR,
)
from metricflow_semantics.test_helpers.semantic_manifest_yamls.scd_manifest import SCD_MANIFEST_ANCHOR
from metricflow_semantics.test_helpers.semantic_manifest_yamls.simple_manifest import SIMPLE_MANIFEST_ANCHOR
from metricflow_semantics.test_helpers.semantic_manifest_yamls.simple_multi_hop_join_manifest import (
    SIMPLE_MULTI_HOP_JOIN_MANIFEST_ANCHOR,
)
from metricflow_semantics.test_helpers.time_helpers import ConfigurableTimeSource
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.builder.source_node import SourceNodeBuilder, SourceNodeSet
from metricflow.dataflow.nodes.read_sql_source import ReadSqlSourceNode
from metricflow.dataset.convert_semantic_model import SemanticModelToDataSetConverter
from metricflow.dataset.semantic_model_adapter import SemanticModelDataSet
from metricflow.engine.metricflow_engine import MetricFlowEngine
from metricflow.plan_conversion.to_sql_plan.dataflow_to_sql import DataflowToSqlPlanConverter
from metricflow.plan_conversion.to_sql_plan.dataflow_to_subquery import DataflowNodeToSqlSubqueryVisitor
from metricflow.protocols.sql_client import SqlClient

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
    # Where the YAML files are located.
    yaml_file_dir: pathlib.Path


class SemanticManifestSetup(Enum):
    """Enumeration of semantic manifests that defined in YAML files and used for testing."""

    AMBIGUOUS_RESOLUTION_MANIFEST = SemanticManifestSetupPropertySet(
        semantic_manifest_name="ambiguous_resolution_manifest",
        id_number_space=IdNumberSpace.for_block(0),
        yaml_file_dir=AMBIGUOUS_RESOLUTION_MANIFEST_ANCHOR.directory,
    )
    # Not including CONFIG_LINTER_MANIFEST as it has intentional errors for running validations.
    CYCLIC_JOIN_MANIFEST = SemanticManifestSetupPropertySet(
        semantic_manifest_name="cyclic_join_manifest",
        id_number_space=IdNumberSpace.for_block(1),
        yaml_file_dir=CYCLIC_JOIN_MANIFEST_ANCHOR.directory,
    )
    DATA_WAREHOUSE_VALIDATION_MANIFEST = SemanticManifestSetupPropertySet(
        semantic_manifest_name="data_warehouse_validation_manifest",
        id_number_space=IdNumberSpace.for_block(2),
        yaml_file_dir=DW_VALIDATION_MANIFEST_ANCHOR.directory,
    )
    EXTENDED_DATE_MANIFEST = SemanticManifestSetupPropertySet(
        semantic_manifest_name="extended_date_manifest",
        id_number_space=IdNumberSpace.for_block(3),
        yaml_file_dir=EXTENDED_DATE_MANIFEST_ANCHOR.directory,
    )
    JOIN_TYPES_MANIFEST = SemanticManifestSetupPropertySet(
        semantic_manifest_name="join_types_manifest",
        id_number_space=IdNumberSpace.for_block(4),
        yaml_file_dir=JOIN_TYPES_MANIFEST_ANCHOR.directory,
    )
    MULTI_HOP_JOIN_MANIFEST = SemanticManifestSetupPropertySet(
        semantic_manifest_name="multi_hop_join_manifest",
        id_number_space=IdNumberSpace.for_block(5),
        yaml_file_dir=MULTI_HOP_JOIN_MANIFEST_ANCHOR.directory,
    )
    PARTITIONED_MULTI_HOP_JOIN_MANIFEST = SemanticManifestSetupPropertySet(
        semantic_manifest_name="partitioned_multi_hop_join_manifest",
        id_number_space=IdNumberSpace.for_block(6),
        yaml_file_dir=PARTITIONED_MULTI_HOP_JOIN_MANIFEST_ANCHOR.directory,
    )
    NON_SM_MANIFEST = SemanticManifestSetupPropertySet(
        semantic_manifest_name="non_sm_manifest",
        id_number_space=IdNumberSpace.for_block(7),
        yaml_file_dir=NON_SM_MANIFEST_ANCHOR.directory,
    )
    SCD_MANIFEST = SemanticManifestSetupPropertySet(
        semantic_manifest_name="scd_manifest",
        id_number_space=IdNumberSpace.for_block(8),
        yaml_file_dir=SCD_MANIFEST_ANCHOR.directory,
    )
    SIMPLE_MANIFEST = SemanticManifestSetupPropertySet(
        semantic_manifest_name="simple_manifest",
        id_number_space=IdNumberSpace.for_block(9),
        yaml_file_dir=SIMPLE_MANIFEST_ANCHOR.directory,
    )
    SIMPLE_MULTI_HOP_JOIN_MANIFEST = SemanticManifestSetupPropertySet(
        semantic_manifest_name="simple_multi_hop_join_manifest",
        id_number_space=IdNumberSpace.for_block(10),
        yaml_file_dir=SIMPLE_MULTI_HOP_JOIN_MANIFEST_ANCHOR.directory,
    )
    NAME_EDGE_CASE_MANIFEST = SemanticManifestSetupPropertySet(
        semantic_manifest_name="name_edge_case_manifest",
        id_number_space=IdNumberSpace.for_block(11),
        yaml_file_dir=NAME_EDGE_CASE_MANIFEST_ANCHOR.directory,
    )

    @property
    def id_number_space(self) -> IdNumberSpace:  # noqa: D102
        return self.value.id_number_space

    @property
    def semantic_manifest_name(self) -> str:  # noqa: D102
        return self.value.semantic_manifest_name

    @property
    def yaml_file_dir(self) -> pathlib.Path:  # noqa: D102
        return self.value.yaml_file_dir


@dataclass(frozen=True)
class MetricFlowEngineTestFixture:
    """Contains objects for testing the MF engine for a specific semantic manifest."""

    semantic_manifest: PydanticSemanticManifest
    semantic_manifest_lookup: SemanticManifestLookup
    column_association_resolver: ColumnAssociationResolver
    data_set_mapping: OrderedDict[str, SemanticModelDataSet]
    read_node_mapping: OrderedDict[str, ReadSqlSourceNode]
    source_node_set: SourceNodeSet
    dataflow_to_sql_converter: DataflowToSqlPlanConverter
    query_parser: MetricFlowQueryParser
    metricflow_engine: MetricFlowEngine
    source_node_builder: SourceNodeBuilder

    _node_output_resolver: DataflowNodeToSqlSubqueryVisitor

    @staticmethod
    def from_parameters(  # noqa: D102
        sql_client: SqlClient,
        semantic_manifest: PydanticSemanticManifest,
    ) -> MetricFlowEngineTestFixture:
        semantic_manifest_lookup = SemanticManifestLookup(semantic_manifest)
        data_set_mapping = MetricFlowEngineTestFixture._create_data_sets(semantic_manifest_lookup)
        read_node_mapping = MetricFlowEngineTestFixture._data_set_to_read_nodes(data_set_mapping)
        column_association_resolver = DunderColumnAssociationResolver()
        source_node_builder = SourceNodeBuilder(column_association_resolver, semantic_manifest_lookup)
        source_node_set = source_node_builder.create_from_data_sets(list(data_set_mapping.values()))
        node_output_resolver = DataflowNodeToSqlSubqueryVisitor(
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
            dataflow_to_sql_converter=DataflowToSqlPlanConverter(
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
            source_node_builder=source_node_builder,
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
            source_node_builder=self.source_node_builder,
        )

    @staticmethod
    def _data_set_to_read_nodes(
        data_sets: OrderedDict[str, SemanticModelDataSet]
    ) -> OrderedDict[str, ReadSqlSourceNode]:
        """Return a mapping from the name of the semantic model to the dataflow plan node that reads from it."""
        # Moved from model_fixtures.py.
        return_dict: OrderedDict[str, ReadSqlSourceNode] = OrderedDict()
        for semantic_model_name, data_set in data_sets.items():
            return_dict[semantic_model_name] = ReadSqlSourceNode.create(data_set)
            logger.debug(
                LazyFormat(
                    lambda: f"For semantic model {semantic_model_name}, creating node_id {return_dict[semantic_model_name].node_id}"
                )
            )

        return return_dict

    @staticmethod
    def _create_data_sets(
        semantic_manifest_lookup: SemanticManifestLookup,
    ) -> OrderedDict[str, SemanticModelDataSet]:
        """Convert the SemanticModels in the model to SqlDataSets.

        Key is the name of the semantic model, value is the associated data set.
        """
        # Moved from model_fixtures.py.

        # Use ordered dict and sort by name to get consistency when running tests.
        data_sets = OrderedDict()
        converter = SemanticModelToDataSetConverter(
            column_association_resolver=DunderColumnAssociationResolver(),
            manifest_lookup=semantic_manifest_lookup,
        )
        model_lookup = semantic_manifest_lookup.semantic_model_lookup
        for model_reference, semantic_model in model_lookup.model_reference_to_model.items():
            data_sets[semantic_model.name] = converter.create_sql_source_data_set(model_reference)

        return data_sets


@pytest.fixture(scope="session")
def mf_engine_test_fixture_mapping(
    template_mapping: Dict[str, str],
    sql_client: SqlClient,
) -> Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]:
    """Returns a mapping for all semantic manifests used in testing to the associated test fixture."""
    fixture_mapping: Dict[SemanticManifestSetup, MetricFlowEngineTestFixture] = {}
    for semantic_manifest_setup in SemanticManifestSetup:
        with SequentialIdGenerator.id_number_space(semantic_manifest_setup.id_number_space.start_value):
            fixture_mapping[semantic_manifest_setup] = MetricFlowEngineTestFixture.from_parameters(
                sql_client,
                mf_load_manifest_from_yaml_directory(semantic_manifest_setup.yaml_file_dir, template_mapping),
            )

    return fixture_mapping


@pytest.fixture(scope="session")
def template_mapping(mf_test_configuration: MetricFlowTestConfiguration) -> Dict[str, str]:
    """Mapping for template variables in the model YAML files."""
    return {"source_schema": mf_test_configuration.mf_source_schema}


@pytest.fixture(scope="session")
def simple_semantic_manifest_lookup_non_ds(  # noqa: D103
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> SemanticManifestLookup:
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.NON_SM_MANIFEST].semantic_manifest_lookup


@pytest.fixture(scope="session")
def simple_semantic_manifest_lookup(  # noqa: D103
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> SemanticManifestLookup:
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].semantic_manifest_lookup


@pytest.fixture(scope="session")
def partitioned_multi_hop_join_semantic_manifest_lookup(  # noqa: D103
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> SemanticManifestLookup:
    return mf_engine_test_fixture_mapping[
        SemanticManifestSetup.PARTITIONED_MULTI_HOP_JOIN_MANIFEST
    ].semantic_manifest_lookup


@pytest.fixture(scope="session")
def multi_hop_join_semantic_manifest_lookup(  # noqa: D103
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
def multi_hop_join_semantic_manifest(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> PydanticSemanticManifest:
    """Model used for many tests."""
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.MULTI_HOP_JOIN_MANIFEST].semantic_manifest


@pytest.fixture(scope="session")
def extended_date_semantic_manifest_lookup(  # noqa: D103
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
def ambiguous_resolution_manifest_lookup(  # noqa: D103
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> SemanticManifestLookup:
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.AMBIGUOUS_RESOLUTION_MANIFEST].semantic_manifest_lookup


@pytest.fixture(scope="session")
def simple_multi_hop_join_manifest(  # noqa: D103
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> PydanticSemanticManifest:
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MULTI_HOP_JOIN_MANIFEST].semantic_manifest


@pytest.fixture(scope="session")
def simple_multi_hop_join_manifest_lookup(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> SemanticManifestLookup:
    """Manifest used to test ambiguous resolution of group-by-items."""
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MULTI_HOP_JOIN_MANIFEST].semantic_manifest_lookup


@pytest.fixture(scope="session")
def name_edge_case_manifest(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> SemanticManifestLookup:
    """Manifest used to test name-related edge cases."""
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.NAME_EDGE_CASE_MANIFEST].semantic_manifest_lookup
