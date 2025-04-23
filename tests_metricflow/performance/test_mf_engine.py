from __future__ import annotations

import logging
import time
from typing import Optional

import pytest
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.test_utils import as_datetime
from dbt_semantic_interfaces.transformations.semantic_manifest_transformer import PydanticSemanticManifestTransformer
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.model.semantics.linkable_spec_index import LinkableSpecIndex
from metricflow_semantics.model.semantics.linkable_spec_index_builder import LinkableSpecIndexBuilder
from metricflow_semantics.model.semantics.manifest_object_lookup import SemanticManifestObjectLookup
from metricflow_semantics.model.semantics.semantic_model_join_evaluator import MAX_JOIN_HOPS
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.dunder_column_association_resolver import DunderColumnAssociationResolver
from metricflow_semantics.test_helpers.time_helpers import ConfigurableTimeSource
from tests_metricflow_semantics.model.test_semantic_model_container import build_semantic_model_lookup_from_manifest

from metricflow.engine.metricflow_engine import MetricFlowEngine
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.performance.semantic_manifest_generator import SyntheticManifestGenerator
from tests_metricflow.performance.synthetic_manifest_parameter_set import SyntheticManifestParameterSet

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
