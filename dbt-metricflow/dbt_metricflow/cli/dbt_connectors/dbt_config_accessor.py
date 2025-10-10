from __future__ import annotations

import dataclasses
import logging
import textwrap
from pathlib import Path
from typing import List, Type

from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.factory import get_adapter_by_type
from dbt.cli.main import dbtRunner
from dbt.config.profile import Profile
from dbt.config.project import Project
from dbt.config.runtime import load_profile, load_project
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from metricflow_semantics.model.dbt_manifest_parser import parse_manifest_from_dbt_generated_manifest
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from typing_extensions import Self

from dbt_metricflow.cli.cli_errors import LoadSemanticManifestException

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class dbtPaths:
    """Bundle of dbt configuration paths."""

    model_paths: List[str]
    seed_paths: List[str]
    target_path: str


@dataclasses.dataclass
class dbtProjectMetadata:
    """Container to access dbt project metadata such as dbt_project.yml and profiles.yml."""

    profile: Profile
    project: Project
    project_path: Path

    @classmethod
    def load_from_paths(cls: Type[Self], profiles_path: Path, project_path: Path) -> Self:
        """Loads all dbt artifacts for the project associated with the given profile / project path.

        Note: calling this method can update global state in the dbt libraries.
        """
        profiles_path_str = str(profiles_path)
        project_path_str = str(project_path)

        logger.debug(
            LazyFormat("Loading dbt project metadata", profiles_path=profiles_path_str, project_path=project_path_str)
        )
        # The `debug` command runs a few validations on the project
        # See: https://docs.getdbt.com/reference/commands/debug
        #
        # `--quiet` will show only error logs and suppress non-error logs.
        #
        # Running this command also has a side effect of mutating some global state in the dbt libraries.
        # e.g. interacts with `load_profile` and adapters. Without this, `load_profile` throws an exception.
        validation_result = dbtRunner().invoke(
            ["debug", "--quiet", "--profiles-dir", profiles_path_str, "--project-dir", project_path_str]
        )

        logger.debug(LazyFormat("Ran validation", validation_result=validation_result))
        if validation_result.exception is not None:
            raise RuntimeError(
                LazyFormat(
                    "Error validating the dbt project",
                    profiles_path=profiles_path_str,
                    project_path=project_path_str,
                )
            ) from validation_result.exception

        profile = load_profile(project_root=project_path_str, cli_vars={})
        logger.debug(LazyFormat("Loaded profile", profile=profile))

        project = load_project(project_path_str, version_check=False, profile=profile)
        logger.debug(LazyFormat("Loaded project", project_name=project.project_name))
        return cls(profile=profile, project=project, project_path=project_path)

    @property
    def dbt_paths(self) -> dbtPaths:
        """Return the bundle of configuration paths."""
        return dbtPaths(
            model_paths=self.project.model_paths,
            seed_paths=self.project.seed_paths,
            target_path=self.project.target_path,
        )

    @property
    def schema(self) -> str:
        """Return the adapter schema."""
        return self.profile.credentials.schema


@dataclasses.dataclass
class dbtArtifacts:
    """Container with access to the dbt artifacts required to power the MetricFlow CLI.

    In order to avoid double-loading this should generally be built from the dbtProjectMetadata struct.
    This does not inherit because it is a slightly different struct. In most cases this is the object
    we want to reference.
    """

    profile: Profile
    project: Project
    adapter: BaseAdapter
    semantic_manifest: SemanticManifest

    @classmethod
    def load_from_project_metadata(cls: Type[Self], project_metadata: dbtProjectMetadata) -> Self:
        """Loads adapter and semantic manifest associated with the previously-fetched project metadata."""
        # dbt's get_adapter helper expects an AdapterRequiredConfig, but `project` is missing cli_vars
        # In practice, get_adapter only actually requires HasCredentials, so we replicate the type extraction
        # from get_adapter here rather than spinning up a full RuntimeConfig instance
        # TODO: Move to a fully supported interface when one becomes available
        adapter = get_adapter_by_type(project_metadata.profile.credentials.type)
        semantic_manifest = dbtArtifacts.build_semantic_manifest_from_dbt_project_root(
            project_root=project_metadata.project_path
        )
        return cls(
            profile=project_metadata.profile,
            project=project_metadata.project,
            adapter=adapter,
            semantic_manifest=semantic_manifest,
        )

    @staticmethod
    def build_semantic_manifest_from_dbt_project_root(project_root: Path) -> SemanticManifest:
        """In the dbt project root, retrieve the manifest path and parse the SemanticManifest."""
        DEFAULT_TARGET_PATH = "target/semantic_manifest.json"
        full_path_to_manifest = Path(project_root, DEFAULT_TARGET_PATH).resolve()
        if not full_path_to_manifest.exists():
            raise LoadSemanticManifestException(
                "\n".join(
                    textwrap.wrap(
                        "Please ensure that you are running `mf` in the root directory of a dbt project "
                        "and that the semantic manifest artifact exists. If this is your first time running "
                        "`mf`, run `dbt parse` or `dbt build` to generate the artifact.",
                        width=80,
                    )
                )
            )
        try:
            with open(full_path_to_manifest, "r") as file:
                raw_contents = file.read()
                return parse_manifest_from_dbt_generated_manifest(manifest_json_string=raw_contents)
        except Exception as e:
            raise LoadSemanticManifestException from e
