from __future__ import annotations

import logging
import textwrap

from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.parsing.dir_to_model import parse_yaml_files_to_validation_ready_semantic_manifest
from dbt_semantic_interfaces.parsing.objects import YamlConfigFile
from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.validations.semantic_manifest_validator import SemanticManifestValidator
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_logging.pretty_print import mf_pformat

logger = logging.getLogger(__name__)


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


def log_query_spec() -> None:  # noqa: D103
    semantic_manifest = _semantic_manifest()
    query_parser = MetricFlowQueryParser(SemanticManifestLookup(semantic_manifest))
    query_spec = query_parser.parse_and_validate_query(
        metric_names=["bookings"], group_by_names=["booking__is_instant"]
    ).query_spec

    logger.debug(LazyFormat(lambda: f"{query_spec.__class__.__name__}:\n{mf_pformat(query_spec)}"))


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s %(levelname)s %(filename)s:%(lineno)d - %(message)s", level=logging.INFO)
    log_query_spec()
