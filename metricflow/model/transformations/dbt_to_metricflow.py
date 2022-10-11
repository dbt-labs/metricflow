from dataclasses import dataclass
from operator import xor
import traceback
from typing import Dict, List, Set
from dbt.contracts.graph.parsed import ParsedMetric as DbtMetric, ParsedModelNode as DbtModelNode
from dbt.exceptions import ref_invalid_args
from dbt.contracts.graph.manifest import Manifest as DbtManifest
from metricflow.aggregation_properties import AggregationType
from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.elements.dimension import Dimension, DimensionType, DimensionTypeParams
from metricflow.model.objects.elements.identifier import Identifier
from metricflow.model.objects.elements.measure import Measure
from metricflow.model.objects.metric import Metric
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.parsing.dir_to_model import ModelBuildResult
from metricflow.model.validations.validator_helpers import ModelValidationResults, ValidationError, ValidationIssue
from metricflow.time.time_granularity import TimeGranularity


@dataclass
class TransformedDbtMetric:  # noqa: D
    data_source: DataSource
    metrics: List[Metric]


CALC_METHOD_TO_MEASURE_TYPE: Dict[str, AggregationType] = {
    "count": AggregationType.COUNT,
    "count_distinct": AggregationType.COUNT_DISTINCT,
    "sum": AggregationType.SUM,
    "average": AggregationType.AVERAGE,
    "min": AggregationType.MIN,
    "max": AggregationType.MAX,
    # "derived": AggregationType.DERIVED # Derived DBT metrics don't create measures
}


def _resolve_metric_model_ref(manifest: DbtManifest, dbt_metric: DbtMetric) -> DbtModelNode:  # noqa: D
    if dbt_metric.model[:4] != "ref(":
        raise RuntimeError("Can only resolve refs for ref strings that begin with `ref(`")
    ref_parts = dbt_metric.model[4:-1].split(",")
    target_model = None
    target_package = None

    if len(ref_parts) == 1:
        target_model = ref_parts[0].strip(" \"'\t\r\n")
    elif len(ref_parts) == 2:
        target_package = ref_parts[0].strip(" \"'\t\r\n")
        target_model = ref_parts[1].strip(" \"'\t\r\n")
    else:
        ref_invalid_args(dbt_metric.name, ref_parts)

    node = manifest.resolve_ref(
        target_model_name=target_model,
        target_model_package=target_package,
        current_project=manifest.metadata.project_id,
        node_package=dbt_metric.package_name,
    )
    assert isinstance(
        node, DbtModelNode
    ), f"Ref `{dbt_metric.model}` resolved to {node}, which is not of type `{DbtModelNode.__name__}`"
    return node


def _db_table_from_model_node(node: DbtModelNode) -> str:  # noqa: D
    return f"{node.database}.{node.schema}.{node.name}"


def _build_dimensions(dbt_metric: DbtMetric) -> List[Dimension]:  # noqa: D
    dimensions = []

    # Build dimensions specifically from DbtMetric.dimensions list
    for dimension in dbt_metric.dimensions:
        dimensions.append(
            Dimension(
                name=dimension,
                type=DimensionType.CATEGORICAL,
            )
        )

    # Add DbtMetric.timestamp as a time dimension
    dimensions.append(
        Dimension(
            name=dbt_metric.timestamp,
            type=DimensionType.TIME,
            type_params=DimensionTypeParams(time_granularity=TimeGranularity.DAY),
        )
    )

    # We need to deduplicate the filters because a field could be the same in
    # two filters. For example, if two filters exist for `amount`, one with
    # `>= 500` and the other `< 1000`, but only one dimension should be created
    distinct_dbt_metric_filter_fields = set([filter.field for filter in dbt_metric.filters])
    # Add dimension per distinct filter field
    # exclude when field is also listed as a DbtMetric.dimension
    # exclude when field is also the DbtMetric.timestamp
    for filter_field in distinct_dbt_metric_filter_fields:
        if filter_field not in dbt_metric.dimensions and filter_field != dbt_metric.timestamp:
            dimensions.append(
                Dimension(
                    name=filter_field,
                    type=DimensionType.CATEGORICAL,
                )
            )

    return dimensions


def _build_measure(dbt_metric: DbtMetric) -> Measure:  # noqa: D
    return Measure(
        name=dbt_metric.name, agg=CALC_METHOD_TO_MEASURE_TYPE[dbt_metric.calculation_method], expr=dbt_metric.expression
    )


def _build_data_source(dbt_metric: DbtMetric, manifest: DbtManifest) -> DataSource:  # noqa: D
    metric_model_ref: DbtModelNode = _resolve_metric_model_ref(
        manifest=manifest,
        dbt_metric=dbt_metric,
    )
    data_source_table = _db_table_from_model_node(metric_model_ref)
    return DataSource(
        name=metric_model_ref.name,
        description=metric_model_ref.description,
        sql_table=data_source_table,
        dbt_model=data_source_table,
        dimensions=_build_dimensions(dbt_metric),
        measures=[_build_measure(dbt_metric)],
    )


def dbt_metric_to_metricflow_elements(dbt_metric: DbtMetric, manifest: DbtManifest) -> TransformedDbtMetric:  # noqa: D
    data_source = _build_data_source(dbt_metric, manifest)
    return TransformedDbtMetric(data_source=data_source, metrics=[])


def merge_data_sources(data_sources: List[DataSource]) -> DataSource:  # noqa: D
    if len(data_sources) == 1:
        return data_sources[0]

    measures: Set[Measure] = set()
    identifiers: Set[Identifier] = set()
    dimensions: Set[Dimension] = set()
    names: List[str] = []
    descriptions: List[str] = []
    sql_tables: List[str] = []
    sql_queries: List[str] = []
    dbt_models: List[str] = []
    for data_source in data_sources:
        names.append(data_source.name) if data_source.name else None
        descriptions.append(data_source.description) if data_source.description else None
        sql_tables.append(data_source.sql_table) if data_source.sql_table else None
        sql_queries.append(data_source.sql_query) if data_source.sql_query else None
        dbt_models.append(data_source.dbt_model) if data_source.dbt_model else None
        measures = measures.union(set(data_source.measures)) if data_source.measures else measures
        identifiers = identifiers.union(set(data_source.identifiers)) if data_source.identifiers else identifiers
        dimensions = dimensions.union(set(data_source.dimensions)) if data_source.dimensions else dimensions

    set_names = set(names)
    set_descriptions = set(descriptions)
    set_sql_tables = set(sql_tables)
    set_sql_queries = set(sql_queries)
    set_dbt_models = set(dbt_models)

    assert len(set_names) == 1, "Cannot merge data sources, all data sources to merge must have same name"
    assert (
        len(set_descriptions) <= 1
    ), "Cannot merge data sources, all data sources to merge must have same descritpion (or none)"
    assert (
        len(set_sql_tables) <= 1
    ), "Cannot merge data sources, all data sources to merge must have same sql_table (or none)"
    assert (
        len(set_sql_queries) <= 1
    ), "Cannot merge data sources, all data sources to merge must have same sql_query (or none)"
    assert xor(
        len(set_sql_tables) == 1, len(set_sql_queries) == 1
    ), "Cannot merge data sources, definitions for both sql_table and sql_query exist"
    assert (
        len(set_dbt_models) <= 1
    ), "Cannot merge data sources, all data sources to merge must have same dbt_model (or none)"

    return DataSource(
        name=list(set_names)[0],
        description=list(set_descriptions)[0] if set_descriptions else None,
        sql_table=list(set_sql_tables)[0] if set_sql_tables else None,
        sql_query=list(set_sql_queries)[0] if set_sql_queries else None,
        dbt_model=list(set_dbt_models)[0] if set_dbt_models else None,
        dimensions=list(dimensions),
        identifiers=list(identifiers),
        measures=list(measures),
    )


def transform_manifest_into_user_configured_model(manifest: DbtManifest) -> ModelBuildResult:  # noqa: D
    data_sources_map: Dict[str, List[DataSource]] = {}
    metrics = []
    issues: List[ValidationIssue] = []
    for dbt_metric in manifest.metrics.values():
        # TODO: Handle derived dbt metrics
        if dbt_metric.calculation_method == "derived":
            continue
        else:
            transformed_dbt_metric = dbt_metric_to_metricflow_elements(dbt_metric=dbt_metric, manifest=manifest)
            if transformed_dbt_metric.data_source.name not in data_sources_map:
                data_sources_map[transformed_dbt_metric.data_source.name] = [transformed_dbt_metric.data_source]
            else:
                data_sources_map[transformed_dbt_metric.data_source.name].append(transformed_dbt_metric.data_source)
            metrics.extend(transformed_dbt_metric.metrics)

    # As it might be the case that we generated many of the same data source,
    # we need to merge / dedupe them
    deduped_data_sources = []
    for name, data_sources in data_sources_map.items():
        try:
            deduped_data_sources.append(merge_data_sources(data_sources))
        except Exception as e:
            issues.append(
                ValidationError(
                    message=f"Failed to merge data sources with the name `{name}`",
                    extra_detail="".join(traceback.format_tb(e.__traceback__)),
                )
            )

    return ModelBuildResult(
        model=UserConfiguredModel(data_sources=list(deduped_data_sources), metrics=metrics),
        issues=ModelValidationResults.from_issues_sequence(issues=issues),
    )
