from dataclasses import dataclass
from dbt.lib import get_dbt_config
from dbt import tracking
from dbt.parser.manifest import ManifestLoader
from dbt.contracts.graph.manifest import Manifest, UserConfiguredModel
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
) -> Manifest:
    """Builds the dbt Manifest object from the dbt project"""

    profile_args = DbtProfileArgs(profile=profile, target=target)
    dbt_config = get_dbt_config(project_dir=directory, args=profile_args)
    # If we don't disable tracking, we have to setup a full
    # dbt User object to build the manifest
    tracking.disable_tracking()
    return ManifestLoader.get_full_manifest(config=dbt_config)

def get_dbt_user_configured_model(
 directory: str, profile: Optional[str] = None, target: Optional[str] = None
) -> UserConfiguredModel:
    """Returns the user configured Model from the dbt Manifest"""
    
    manifest = get_dbt_project_manifest(
        directory="/Users/callummccann/repos/dbt-core/testing-project/postgres",
        profile=profile,
        target=target
    )

    return manifest.user_configured_model