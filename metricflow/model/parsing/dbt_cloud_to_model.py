from typing import List

from dbt_metadata_client.client import Client, Operation
from dbt_metadata_client.dbt_metadata_api_schema import dbt_metadata_api_schema, MetricNode

from dbt_semantic_interfaces.model_transformer import ModelTransformer
from dbt_semantic_interfaces.parsing.dir_to_model import ModelBuildResult
from metricflow.model.dbt_converter import DbtConverter


def get_dbt_cloud_metrics(auth: str, job_id: str) -> List[MetricNode]:
    """Builds the dbt Manifest object from the dbt project"""

    client = Client(api_token=auth)
    query_op = Operation(dbt_metadata_api_schema.Query)
    metrics = query_op.metrics(job_id=job_id)
    # specify the attributes we need on metrics
    metrics.calculation_method()
    metrics.depends_on()
    metrics.description()
    metrics.dimensions()
    metrics.expression()
    metrics.filters()
    metrics.name()
    metrics.timestamp()
    metrics.type()
    model = metrics.model()
    # specify the attributes we need on models
    model.alias()
    model.columns()
    model.database()
    model.description()
    model.name()
    model.schema()
    model.type()

    data = client.query_operation(operation=query_op)
    metric_nodes: List[MetricNode] = (query_op + data).metrics
    return metric_nodes


def parse_dbt_cloud_metrics_to_model(dbt_metrics: List[MetricNode]) -> ModelBuildResult:
    """Builds a SemanticManifest from a list of dbt cloud MetricNodes"""
    build_result = DbtConverter().convert(dbt_metrics=tuple(dbt_metrics))
    transformed_model = ModelTransformer.transform(model=build_result.model)
    return ModelBuildResult(model=transformed_model, issues=build_result.issues)


def model_build_result_for_dbt_cloud_job(auth: str, job_id: str) -> ModelBuildResult:
    """Combines `get_dbt_cloud_metrics` and `parse_dbt_cloud_metrics_to_model` to get a ModelBuildResult"""
    dbt_metrics = get_dbt_cloud_metrics(auth=auth, job_id=job_id)
    return parse_dbt_cloud_metrics_to_model(dbt_metrics=dbt_metrics)
