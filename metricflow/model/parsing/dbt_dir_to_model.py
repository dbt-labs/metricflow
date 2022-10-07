from dbt.lib import get_dbt_config
from dbt import tracking
from dbt.parser.manifest import ManifestLoader as DbtManifestLoader, Manifest as DbtManifest
from metricflow.model.parsing.dir_to_model import ModelBuildResult
from metricflow.model.transformations.dbt_to_metricflow import transform_manifest_into_user_configured_model


def get_dbt_project_manifest(directory: str) -> DbtManifest:
    """Builds the dbt Manifest object from the dbt project"""

    dbt_config = get_dbt_config(project_dir=directory)
    # If we don't disable tracking, we have to setup a full
    # dbt User object to build the manifest
    tracking.disable_tracking()
    return DbtManifestLoader.get_full_manifest(config=dbt_config)


def parse_dbt_project_to_model(directory: str) -> ModelBuildResult:
    """Parse dbt model files in the given directory to a UserConfiguredModel."""
    manifest = get_dbt_project_manifest(directory=directory)
    return transform_manifest_into_user_configured_model(manifest=manifest)
