from __future__ import annotations

import cProfile
import logging
import time
from typing import Optional

import pytest
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.test_utils import as_datetime
from dbt_semantic_interfaces.transformations.semantic_manifest_transformer import PydanticSemanticManifestTransformer
from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.dsi.manifest_object_lookup import (
    ManifestObjectLookup as NewManifestObjectLookup,
)
from metricflow_semantics.experimental.mf_graph.path_finding.pathfinder import MetricflowPathfinder
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.recipe_writer_path import (
    AttributeRecipeWriterPath,
)
from metricflow_semantics.experimental.semantic_graph.builder.graph_builder import SemanticGraphBuilder
from metricflow_semantics.experimental.semantic_graph.sg_interfaces import SemanticGraphEdge, SemanticGraphNode
from metricflow_semantics.experimental.singleton import Singleton
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.mf_logging.runtime import log_block_runtime
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.model.semantics.linkable_spec_index import LinkableSpecIndex
from metricflow_semantics.model.semantics.linkable_spec_index_builder import LinkableSpecIndexBuilder
from metricflow_semantics.model.semantics.linkable_spec_resolver import LegacyLinkableSpecResolver
from metricflow_semantics.model.semantics.manifest_object_lookup import SemanticManifestObjectLookup
from metricflow_semantics.model.semantics.semantic_model_join_evaluator import MAX_JOIN_HOPS
from metricflow_semantics.model.semantics.semantic_model_lookup import SemanticModelLookup
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.dunder_column_association_resolver import DunderColumnAssociationResolver
from metricflow_semantics.test_helpers.synthetic_manifest.semantic_manifest_generator import SyntheticManifestGenerator
from metricflow_semantics.test_helpers.synthetic_manifest.synthetic_manifest_parameter_set import (
    SyntheticManifestParameterSet,
)
from metricflow_semantics.test_helpers.time_helpers import ConfigurableTimeSource
from metricflow_semantics.time.time_spine_source import TimeSpineSource
from run_pstats import CPROFILE_OUTPUT_FILE_PATH
from tests_metricflow_semantics.model.test_semantic_model_container import build_semantic_model_lookup_from_manifest

from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlClient

logger = logging.getLogger(__name__)


@pytest.mark.slow
def test_init_time(sql_client: SqlClient) -> None:
    """Test that the MF engine init time is 10x faster when initialized with the index."""
    parameter_set = SyntheticManifestParameterSet(
        measure_semantic_model_count=20,
        measures_per_semantic_model=20,
        dimension_semantic_model_count=20,
        categorical_dimensions_per_semantic_model=10,
        max_metric_depth=1,
        max_metric_width=400,
        saved_query_count=0,
        metrics_per_saved_query=0,
        categorical_dimensions_per_saved_query=0,
    )
    generator = SyntheticManifestGenerator(parameter_set)
    semantic_manifest = generator.generate_manifest()
    semantic_manifest = PydanticSemanticManifestTransformer.transform(semantic_manifest)
    column_association_resolver = DunderColumnAssociationResolver()

    # Measure the time it takes to initialize the engine without the index.
    init_time_without_index = _time_mf_engine_init(
        sql_client=sql_client,
        column_association_resolver=column_association_resolver,
        semantic_manifest=semantic_manifest,
        linkable_spec_index=None,
    )

    # Measure the time it takes to initialize the engine with the index.
    linkable_spec_index_builder = LinkableSpecIndexBuilder(
        semantic_manifest=semantic_manifest,
        semantic_model_lookup=build_semantic_model_lookup_from_manifest(semantic_manifest),
        manifest_object_lookup=SemanticManifestObjectLookup(semantic_manifest),
        max_entity_links=MAX_JOIN_HOPS,
    )
    linkable_spec_index = linkable_spec_index_builder.build_index()
    init_time_with_index = _time_mf_engine_init(
        sql_client=sql_client,
        column_association_resolver=column_association_resolver,
        semantic_manifest=semantic_manifest,
        linkable_spec_index=linkable_spec_index,
    )

    # Check that the init time with the index is 10x faster.
    speed_up_factor = init_time_without_index / init_time_with_index
    logger.debug(
        LazyFormat(
            "Computed speed up using the index",
            speed_up_factor=speed_up_factor,
            init_time_without_index=init_time_without_index,
            init_time_with_index=init_time_with_index,
        )
    )
    assert speed_up_factor > 10


def _time_mf_engine_init(
    sql_client: SqlClient,
    column_association_resolver: ColumnAssociationResolver,
    semantic_manifest: SemanticManifest,
    linkable_spec_index: Optional[LinkableSpecIndex],
) -> float:
    start_time = time.perf_counter()

    if linkable_spec_index is None:
        semantic_manifest_lookup = SemanticManifestLookup(semantic_manifest)
    else:
        semantic_manifest_lookup = SemanticManifestLookup(semantic_manifest, linkable_spec_index=linkable_spec_index)

    query_parser = MetricFlowQueryParser(semantic_manifest_lookup=semantic_manifest_lookup)
    MetricFlowEngine(
        semantic_manifest_lookup=semantic_manifest_lookup,
        sql_client=sql_client,
        time_source=ConfigurableTimeSource(as_datetime("2020-01-01")),
        query_parser=query_parser,
        column_association_resolver=column_association_resolver,
    )

    return time.perf_counter() - start_time


def _time_original_init(semantic_manifest: SemanticManifest) -> float:
    start_time = time.perf_counter()

    time_spine_sources = TimeSpineSource.build_standard_time_spine_sources(semantic_manifest)
    custom_granularities = TimeSpineSource.build_custom_granularities(time_spine_sources.values())
    manifest_object_lookup = SemanticManifestObjectLookup(semantic_manifest)
    semantic_model_lookup = SemanticModelLookup(semantic_manifest, custom_granularities=custom_granularities)
    linkable_spec_index_builder = LinkableSpecIndexBuilder(
        semantic_manifest=semantic_manifest,
        semantic_model_lookup=semantic_model_lookup,
        manifest_object_lookup=manifest_object_lookup,
        max_entity_links=MAX_JOIN_HOPS,
    )
    linkable_spec_index = linkable_spec_index_builder.build_index()
    resolver = LegacyLinkableSpecResolver(
        semantic_manifest=semantic_manifest,
        semantic_model_lookup=semantic_model_lookup,
        manifest_object_lookup=manifest_object_lookup,
        linkable_spec_index=linkable_spec_index,
    )

    return time.perf_counter() - start_time


def _time_new_init(semantic_manifest: SemanticManifest) -> float:
    start_time = time.perf_counter()

    path_finder = MetricflowPathfinder[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath]()
    manifest_object_lookup = NewManifestObjectLookup(semantic_manifest)
    graph_builder = SemanticGraphBuilder(manifest_object_lookup=manifest_object_lookup)
    semantic_graph = graph_builder.build()

    return time.perf_counter() - start_time


def _time_new_query(
    semantic_manifest: SemanticManifest,
    sql_client: SqlClient,
) -> None:
    manifest_lookup = SemanticManifestLookup(semantic_manifest, use_semantic_graph=True)
    with log_block_runtime("Init engine"):
        mf_engine = MetricFlowEngine(
            semantic_manifest_lookup=manifest_lookup,
            sql_client=sql_client,
        )

    # with log_block_runtime("Run list dimensions #0"):
    #     dimension_count = len(mf_engine.list_dimensions(metric_names=["metric_1_000"]))
    #     logger.info(LazyFormat("Listed dimensions", dimension_count=dimension_count))
    #
    # with log_block_runtime("Run list dimensions #1"):
    #     dimension_count = len(mf_engine.list_dimensions(metric_names=["metric_1_000"]))
    #     logger.info(LazyFormat("Listed dimensions", dimension_count=dimension_count))
    #
    # with log_block_runtime("Run list dimensions #2"):
    #     dimension_count = len(mf_engine.list_dimensions(metric_names=["metric_1_001"]))
    #     logger.info(LazyFormat("Listed dimensions", dimension_count=dimension_count))

    metric_names = ["metric_1_000"]
    group_by_names = ["metric_time", "common_entity__dimension_000"]
    # where_constraints = ["{{ Metric('metric_1_001', group_by=['common_entity']) }}"]
    where_constraints: list[str] = []
    with log_block_runtime("Run explain #0"):
        mf_engine.explain(
            MetricFlowQueryRequest.create_with_random_request_id(
                metric_names=metric_names, group_by_names=group_by_names, where_constraints=where_constraints
            )
        )

    with log_block_runtime("Run explain #1"):
        result = mf_engine.explain(
            MetricFlowQueryRequest.create_with_random_request_id(
                metric_names=metric_names,
                group_by_names=["metric_time", "common_entity__dimension_010"],
                where_constraints=where_constraints,
            )
        )
        # logger.info(LazyFormat("Generated SQL", sql=result.convert_to_execution_plan_result.render_sql_result.sql))


def _time_engine_init(
    semantic_manifest: SemanticManifest,
    sql_client: SqlClient,
) -> float:
    start_time = time.perf_counter()

    manifest_lookup = SemanticManifestLookup(semantic_manifest)
    mf_engine = MetricFlowEngine(
        semantic_manifest_lookup=manifest_lookup,
        sql_client=sql_client,
    )

    return time.perf_counter() - start_time


def test_semantic_graph_init_time(sql_client: SqlClient) -> None:
    # parameter_set = SyntheticManifestParameterSet(
    #     measure_semantic_model_count=20,
    #     measures_per_semantic_model=20,
    #     dimension_semantic_model_count=20,
    #     categorical_dimensions_per_semantic_model=10,
    #     max_metric_depth=3,
    #     max_metric_width=50,
    #     saved_query_count=0,
    #     metrics_per_saved_query=0,
    #     categorical_dimensions_per_saved_query=0,
    # )

    parameter_set = SyntheticManifestParameterSet(
        measure_semantic_model_count=100,
        measures_per_semantic_model=20,
        dimension_semantic_model_count=100,
        categorical_dimensions_per_semantic_model=20,
        max_metric_depth=2,
        max_metric_width=50,
        saved_query_count=0,
        metrics_per_saved_query=0,
        categorical_dimensions_per_saved_query=0,
    )

    # parameter_set = SyntheticManifestParameterSet(
    #     measure_semantic_model_count=1,
    #     measures_per_semantic_model=1,
    #     dimension_semantic_model_count=1,
    #     categorical_dimensions_per_semantic_model=1,
    #     max_metric_depth=2,
    #     max_metric_width=1,
    #     saved_query_count=0,
    #     metrics_per_saved_query=0,
    #     categorical_dimensions_per_saved_query=0,
    # )

    generator = SyntheticManifestGenerator(parameter_set)
    semantic_manifest = generator.generate_manifest()
    semantic_manifest = PydanticSemanticManifestTransformer.transform(semantic_manifest)

    # original_init_time = _time_original_init(semantic_manifest)
    # new_init_time = _time_new_init(semantic_manifest)
    # ratio = original_init_time / new_init_time
    # logger.info(
    #     LazyFormat(
    #         "Compared init times",
    #         original_init_time=original_init_time,
    #         new_init_time=new_init_time,
    #         ratio=f"{ratio:.2f}",
    #     )
    # )

    # output_filename = str(CPROFILE_OUTPUT_FILE_PATH)
    # logger.info(LazyFormat("Running performance profiling", output_filename=output_filename))
    # cProfile.runctx(
    #     statement="_time_new_init(semantic_manifest)",
    #     filename=str(CPROFILE_OUTPUT_FILE_PATH),
    #     locals=locals(),
    #     globals=globals(),
    # )

    # with log_block_runtime("new init"):
    #     _time_new_init(semantic_manifest)

    with log_block_runtime("new query"):
        _time_new_query(semantic_manifest=semantic_manifest, sql_client=sql_client)

    # with log_block_runtime("New engine init"):
    #     _time_engine_init(semantic_manifest, sql_client)

    # with log_block_runtime("original init"):
    #     _time_original_init(semantic_manifest)


@fast_frozen_dataclass()
class DataclassId:
    int_value: int


@fast_frozen_dataclass()
class SingletonId(Singleton):
    int_value: int

    @classmethod
    def get_instance(cls, int_value: int) -> SingletonId:  # noqa: D102
        return cls.get_instance(int_value)


class SingletonFactory:
    _instance_dict: dict[AnyLengthTuple[int], DataclassId] = {}

    def get_instance(self, int_value: int) -> DataclassId:
        key = (int_value,)
        instance = self._instance_dict.get(key)
        if instance is None:
            instance = DataclassId(int_value=int_value)
            self._instance_dict[key] = instance
        return instance


REPEAT_COUNT = 2
ID_COUNT = 200_000


def _test_singleton_dataclass() -> None:
    for _ in range(REPEAT_COUNT):
        for int_value in range(ID_COUNT):
            SingletonId.get_instance(int_value=int_value)


def _test_factory() -> None:
    for _ in range(REPEAT_COUNT):
        for int_value in range(ID_COUNT):
            DataclassId(int_value=int_value)


def test_singleton_approach() -> None:
    cProfile.runctx(
        statement="_test_factory()",
        filename=str(CPROFILE_OUTPUT_FILE_PATH),
        locals=locals(),
        globals=globals(),
    )
