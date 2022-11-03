from dataclasses import dataclass
from dbt_metadata_client.client import Client
from dbt_metadata_client.dbt_metadata_api_schema import MetricNode
from metricflow.model.dbt_transformer import DbtTransformer
from metricflow.model.model_transformer import ModelTransformer
from metricflow.model.parsing.dir_to_model import ModelBuildResult
from typing import List


@dataclass
class DbtCloudJobArgs:
    """Class to represent dbt profile arguments

    dbt's get_dbt_config uses `getattr` to get values out of the passed in args.
    We cannot pass a simple dict, because `getattr` doesn't work for keys of a
    dictionary. Thus we create a simple object that `getattr` will work on.
    """

    auth: str
    job_id: str


def get_dbt_cloud_metrics(job_args: DbtCloudJobArgs) -> list[MetricNode]:
    """Builds the dbt Manifest object from the dbt project"""

    client = Client(api_token=job_args.auth)
    return client.get_metrics(job_id=job_args.job_id)


def parse_dbt_cloud_metrics_to_model(dbt_metrics: List[MetricNode]) -> ModelBuildResult:
    """Builds a UserConfiguredModel from a list of dbt cloud MetricNodes"""
    build_result = DbtTransformer().transform_and_build(dbt_metrics=tuple(dbt_metrics))
    transformed_model = ModelTransformer.pre_validation_transform_model(model=build_result.model)
    transformed_model = ModelTransformer.post_validation_transform_model(model=transformed_model)
    return ModelBuildResult(model=transformed_model, issues=build_result.issues)


def model_build_result_for_dbt_cloud_job(job_args: DbtCloudJobArgs) -> ModelBuildResult:
    """Combines `get_dbt_cloud_metrics` and `parse_dbt_cloud_metrics_to_model` to get a ModelBuildResult"""
    dbt_metrics = get_dbt_cloud_metrics(job_args=job_args)
    return parse_dbt_cloud_metrics_to_model(dbt_metrics=dbt_metrics)
