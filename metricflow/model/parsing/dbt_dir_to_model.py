from dataclasses import dataclass
from dbt.lib import get_dbt_config
from dbt import tracking
from dbt.parser.manifest import ManifestLoader as DbtManifestLoader, Manifest as DbtManifest
from metricflow.model.model_transformer import ModelTransformer
from metricflow.model.parsing.dir_to_model import ModelBuildResult
from metricflow.model.transformations.dbt_to_metricflow import DbtManifestTransformer
from typing import Optional


@dataclass
class DbtProfileArgs:
    """Class to represent dbt profile arguments

    dbt's get_dbt_config uses `getattr` to get values out of the passed in args.
    We cannot pass a simple dict, because `getattr` doesn't work for keys of a
    dictionary. Thus we create a simple object that `getattr` will work on.
    """

    profile: Optional[str] = None
    target: Optional[str] = None


def get_dbt_project_manifest(
    directory: str, profile: Optional[str] = None, target: Optional[str] = None
) -> DbtManifest:
    """Builds the dbt Manifest object from the dbt project"""

    profile_args = DbtProfileArgs(profile=profile, target=target)
    dbt_config = get_dbt_config(project_dir=directory, args=profile_args)
    # If we don't disable tracking, we have to setup a full
    # dbt User object to build the manifest
    tracking.disable_tracking()
    return DbtManifestLoader.get_full_manifest(config=dbt_config)


def parse_dbt_project_to_model(
    directory: str, profile: Optional[str] = None, target: Optional[str] = None
) -> ModelBuildResult:
    """Parse dbt model files in the given directory to a UserConfiguredModel."""
    manifest = get_dbt_project_manifest(directory=directory, profile=profile, target=target)
    build_result = DbtManifestTransformer(manifest=manifest).build_user_configured_model()
    transformed_model = ModelTransformer.pre_validation_transform_model(model=build_result.model)
    transformed_model = ModelTransformer.post_validation_transform_model(model=transformed_model)
    return ModelBuildResult(model=transformed_model, issues=build_result.issues)
