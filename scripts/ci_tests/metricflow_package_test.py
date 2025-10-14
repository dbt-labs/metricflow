from __future__ import annotations

import logging
import textwrap
from collections import OrderedDict

from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.parsing.dir_to_model import parse_yaml_files_to_validation_ready_semantic_manifest
from dbt_semantic_interfaces.parsing.objects import YamlConfigFile
from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.validations.semantic_manifest_validator import SemanticManifestValidator
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.dunder_column_association_resolver import DunderColumnAssociationResolver
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.builder.source_node import SourceNodeBuilder, SourceNodeSet
from metricflow.dataflow.nodes.read_sql_source import ReadSqlSourceNode
from metricflow.dataset.convert_semantic_model import SemanticModelToDataSetConverter
from metricflow.dataset.semantic_model_adapter import SemanticModelDataSet
from metricflow.engine.metricflow_engine import MetricFlowEngine
from metricflow.plan_conversion.to_sql_plan.dataflow_to_subquery import DataflowNodeToSqlSubqueryVisitor

logger = logging.getLogger(__name__)


def _data_set_to_read_nodes(data_sets: OrderedDict[str, SemanticModelDataSet]) -> OrderedDict[str, ReadSqlSourceNode]:
    """Return a mapping from the name of the semantic model to the dataflow plan node that reads from it."""
    # Moved from model_fixtures.py.
    return_dict: OrderedDict[str, ReadSqlSourceNode] = OrderedDict()
    for semantic_model_name, data_set in data_sets.items():
        return_dict[semantic_model_name] = ReadSqlSourceNode.create(data_set)

    return return_dict


def _data_set_to_source_node_set(
    column_association_resolver: ColumnAssociationResolver,
    semantic_manifest_lookup: SemanticManifestLookup,
    data_sets: OrderedDict[str, SemanticModelDataSet],
) -> SourceNodeSet:
    # Moved from model_fixtures.py.
    source_node_builder = SourceNodeBuilder(column_association_resolver, semantic_manifest_lookup)
    return source_node_builder.create_from_data_sets(list(data_sets.values()))


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


def _semantic_manifest() -> PydanticSemanticManifest:
    bookings_yaml_file = YamlConfigFile(
        filepath="dummy_path_0",
        contents=textwrap.dedent(
            """\
            semantic_model:
              name: bookings_source

              node_relation:
                schema_name: some_schema
                alias: bookings_source_table

              defaults:
                agg_time_dimension: ds

              measures:
                - name: bookings
                  expr: "1"
                  agg: sum
                  create_metric: true

              dimensions:
                - name: is_instant
                  type: categorical
                - name: ds
                  type: time
                  type_params:
                    time_granularity: day

              primary_entity: booking

              entities:
                - name: listing
                  type: foreign
                  expr: listing_id
            """
        ),
    )

    project_configuration_yaml_file = YamlConfigFile(
        filepath="projection_configuration_yaml_file_path",
        contents=textwrap.dedent(
            """\
            project_configuration:
              time_spine_table_configurations:
                - location: example_schema.example_table
                  column_name: ds
                  grain: day
            """
        ),
    )

    semantic_manifest = parse_yaml_files_to_validation_ready_semantic_manifest(
        [bookings_yaml_file, project_configuration_yaml_file], apply_transformations=True
    ).semantic_manifest

    SemanticManifestValidator[SemanticManifest]().checked_validations(semantic_manifest)
    return semantic_manifest


def log_dataflow_plan() -> None:  # noqa: D103
    semantic_manifest = _semantic_manifest()
    semantic_manifest_lookup = SemanticManifestLookup(semantic_manifest)
    data_set_mapping = _create_data_sets(semantic_manifest_lookup)
    column_association_resolver = DunderColumnAssociationResolver()

    source_node_builder = SourceNodeBuilder(column_association_resolver, semantic_manifest_lookup)
    source_node_set = source_node_builder.create_from_data_sets(list(data_set_mapping.values()))
    node_output_resolver = DataflowNodeToSqlSubqueryVisitor(
        column_association_resolver=column_association_resolver,
        semantic_manifest_lookup=semantic_manifest_lookup,
    )
    node_output_resolver.cache_output_data_sets(source_node_set.all_nodes)

    dataflow_plan_builder = DataflowPlanBuilder(
        source_node_set=source_node_set,
        semantic_manifest_lookup=semantic_manifest_lookup,
        node_output_resolver=node_output_resolver,
        column_association_resolver=column_association_resolver,
        source_node_builder=source_node_builder,
    )

    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="is_instant",
                    entity_links=(EntityReference("booking"),),
                ),
            ),
        )
    )

    logger.debug(LazyFormat(lambda: f"Dataflow plan is:\n{dataflow_plan.structure_text()}"))


def check_engine_import(metricflow_engine: MetricFlowEngine) -> None:
    """Doesn't need to run, but having this here means that the import is tested."""
    logger.debug(LazyFormat(lambda: f"Engine is {metricflow_engine}"))


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s %(levelname)s %(filename)s:%(lineno)d - %(message)s", level=logging.INFO)
    log_dataflow_plan()
