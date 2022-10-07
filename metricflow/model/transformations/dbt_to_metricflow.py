from typing import List, Union
from dbt.contracts.graph.parsed import ParsedMetric as DbtMetric, ParsedModelNode as DbtModelNode
from dbt.exceptions import ref_invalid_args
from dbt.contracts.graph.manifest import Manifest as DbtManifest
from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.elements.dimension import Dimension, DimensionType
from metricflow.model.objects.metric import Metric
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.parsing.dir_to_model import ModelBuildResult


def _resolve_metric_model_ref(manifest: DbtManifest, dbt_metric: DbtMetric) -> DbtModelNode:  # noqa: D
    if dbt_metric.model[:4] != "ref(":
        raise RuntimeError("Can only resolve refs for ref strings that begin with `ref(`")
    ref_parts = dbt_metric.model[4:-1].split(",")
    target_model = None
    target_package = None

    if len(ref_parts) == 1:
        target_model = ref_parts[0].strip()
    elif len(ref_parts) == 2:
        target_package = ref_parts[0].strip()
        target_model = ref_parts[1].strip()
    else:
        ref_invalid_args(dbt_metric.name, ref_parts)

    node = manifest.resolve_ref(
        target_model_name=target_model,
        target_model_package=target_package,
        current_project=manifest.metadata.project_id,
        node_package=dbt_metric.package_name,
    )
    assert isinstance(node, DbtModelNode)
    return node


def _db_table_from_model_node(node: DbtModelNode) -> str:  # noqa: D
    return f"{node.database}.{node.schema}.{node.name}"


def _build_dimensions(dimensions: List[str]) -> List[Dimension]:  # noqa: D
    built_dimensions = []
    for dimension in dimensions:
        built_dimensions.append(
            Dimension(
                name=dimension,
                type=DimensionType.CATEGORICAL,
            )
        )
    return built_dimensions


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
        dimensions=_build_dimensions(dbt_metric.dimensions),
    )


def dbt_metric_to_metricflow_elements(  # noqa: D
    dbt_metric: DbtMetric, manifest: DbtManifest
) -> List[Union[DataSource, Metric]]:
    data_source = _build_data_source(dbt_metric, manifest)
    return [data_source]


def transform_manifest_into_user_configured_model(manifest: DbtManifest) -> ModelBuildResult:  # noqa: D
    elements = []
    for dbt_metric in manifest.metrics.values():
        elements += dbt_metric_to_metricflow_elements(dbt_metric=dbt_metric, manifest=manifest)
    raise NotImplementedError("`transform_manifest_into_user_configured_model` isn't finished")
    return ModelBuildResult(model=UserConfiguredModel(data_sources=[], metrics=[], materializations=[]))
