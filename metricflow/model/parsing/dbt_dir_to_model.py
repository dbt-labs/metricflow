from dbt.lib import get_dbt_config
from dbt import tracking
from dbt.parser.manifest import ManifestLoader as DbtManifestLoader, Manifest as DbtManifest
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.parsing.dir_to_model import ModelBuildResult


def get_dbt_project_manifest(directory: str) -> DbtManifest:
    """Builds the dbt Manifest object from the dbt project"""

    dbt_config = get_dbt_config(project_dir=directory)
    # If we don't disable tracking, we have to setup a full
    # dbt User object to build the manifest
    tracking.disable_tracking()
    return DbtManifestLoader.get_full_manifest(config=dbt_config)


def parse_dbt_project_to_model(directory: str) -> ModelBuildResult:
    """Parse dbt model files in the given directory to a UserConfiguredModel."""

    manifest = get_dbt_project_manifest(directory=directory)  # noqa: F841

    # TODO: Implement transforming dbt_metrics into a UserConfiguredModel
    raise NotImplementedError("Transforming dbt metrics into a Metricflow UserConfiguredModel has not been implemented")

    return ModelBuildResult(model=UserConfiguredModel(data_sources=[], metrics=[], materializations=[]))
