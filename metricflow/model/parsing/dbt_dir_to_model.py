from dataclasses import dataclass
from dbt.lib import get_dbt_config
from dbt import tracking
from dbt.task.runnable import ManifestTask
from dbt.parser.manifest import ManifestLoader as DbtManifestLoader, Manifest as DbtManifest
from metricflow.model.model_transformer import ModelTransformer
from metricflow.model.parsing.dir_to_model import ModelBuildResult
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

