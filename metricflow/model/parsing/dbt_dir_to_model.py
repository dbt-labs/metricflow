from dbt.lib import get_dbt_config
from dbt.parser.manifest import ManifestLoader as DbtManifestLoader
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.parsing.dir_to_model import ModelBuildResult


def parse_dbt_project_to_model(
    directory: str,
) -> ModelBuildResult:
    """Parse dbt model files in the given directory to a UserConfiguredModel."""
    dbt_config = get_dbt_config(project_dir=directory)
    # dbt_config = DbtRuntimeConfig.new_project(project_root=directory)
    manifest = DbtManifestLoader.get_full_manifest(config=dbt_config)
    dbt_metrics = list(manifest.metrics.values())  # noqa: F841

    # TODO: Implement transforming dbt_metrics into a UserConfiguredModel
    raise NotImplementedError("Transforming dbt metrics into a Metricflow UserConfiguredModel has not been implemented")

    return ModelBuildResult(model=UserConfiguredModel(data_sources=[], metrics=[], materializations=[]))
