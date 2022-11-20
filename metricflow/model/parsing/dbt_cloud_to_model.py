from dbt_metadata_client.client import Client
from dbt_metadata_client.dbt_metadata_api_schema import MetricNode
from metricflow.model.dbt_transformer import DbtTransformer
from metricflow.model.model_transformer import ModelTransformer
from metricflow.model.parsing.dir_to_model import ModelBuildResult
from typing import List


def get_dbt_cloud_metrics(auth: str, job_id: str) -> list[MetricNode]:
    """Builds the dbt Manifest object from the dbt project"""

    client = Client(api_token=auth)
    return client.get_metrics(job_id=job_id)


def parse_dbt_cloud_metrics_to_model(dbt_metrics: List[MetricNode]) -> ModelBuildResult:
    """Builds a UserConfiguredModel from a list of dbt cloud MetricNodes"""
    build_result = DbtTransformer().transform_and_build(dbt_metrics=tuple(dbt_metrics))
    transformed_model = ModelTransformer.pre_validation_transform_model(model=build_result.model)
    transformed_model = ModelTransformer.post_validation_transform_model(model=transformed_model)
    return ModelBuildResult(model=transformed_model, issues=build_result.issues)


def model_build_result_for_dbt_cloud_job(auth: str, job_id: str) -> ModelBuildResult:
    """Combines `get_dbt_cloud_metrics` and `parse_dbt_cloud_metrics_to_model` to get a ModelBuildResult"""
    dbt_metrics = get_dbt_cloud_metrics(auth=auth, job_id=job_id)
    return parse_dbt_cloud_metrics_to_model(dbt_metrics=dbt_metrics)
