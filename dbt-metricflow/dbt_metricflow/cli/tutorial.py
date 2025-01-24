from __future__ import annotations

import pathlib
import shutil
from string import Template
from typing import Sequence


class dbtMetricFlowTutorialHelper:
    """Helper class for managing tutorial related actions (ie., generating sample files)."""

    SAMPLE_DBT_MODEL_DIRECTORY = "sample_dbt_models"
    SAMPLE_MODELS_DIRECTORY = SAMPLE_DBT_MODEL_DIRECTORY + "/sample_models"
    SAMPLE_SEED_DIRECTORY = SAMPLE_DBT_MODEL_DIRECTORY + "/seeds"
    SAMPLE_SEMANTIC_MANIFEST = SAMPLE_DBT_MODEL_DIRECTORY + "/semantic_manifest.json"
    SAMPLE_DBT_PROJECT_DIRECTORY = "mf_tutorial_project"
    SAMPLE_SOURCES_FILE = "sources.yml"

    @staticmethod
    def generate_model_files(model_path: pathlib.Path, profile_schema: str) -> None:
        """Generates the sample model files to the given dbt model path."""
        sample_model_path = pathlib.Path(__file__).parent / dbtMetricFlowTutorialHelper.SAMPLE_MODELS_DIRECTORY
        shutil.copytree(src=sample_model_path, dst=model_path)

        # Generate the sources.yml file with the schema given in profiles.yml
        sample_sources_path = (
            pathlib.Path(__file__).parent
            / dbtMetricFlowTutorialHelper.SAMPLE_DBT_MODEL_DIRECTORY
            / dbtMetricFlowTutorialHelper.SAMPLE_SOURCES_FILE
        )
        with open(sample_sources_path) as file:
            contents = Template(file.read()).substitute({"system_schema": profile_schema})
        dest_sources_path = pathlib.Path(model_path) / dbtMetricFlowTutorialHelper.SAMPLE_SOURCES_FILE
        with open(dest_sources_path, "w") as file:
            file.write(contents)

    @staticmethod
    def generate_seed_files(seed_path: pathlib.Path) -> None:
        """Generates the sample seed files to the given dbt seed path."""
        sample_seed_path = pathlib.Path(__file__).parent / dbtMetricFlowTutorialHelper.SAMPLE_SEED_DIRECTORY
        shutil.copytree(src=sample_seed_path, dst=seed_path)

    @staticmethod
    def generate_semantic_manifest_file(manifest_path: pathlib.Path) -> None:
        """Generates the sample semantic manifest to the given dbt semantic manifest path."""
        target_path = manifest_path.parent
        if not target_path.exists():
            target_path.mkdir()

        sample_manifest_path = pathlib.Path(__file__).parent / dbtMetricFlowTutorialHelper.SAMPLE_SEMANTIC_MANIFEST
        shutil.copy(src=sample_manifest_path, dst=manifest_path)

    @staticmethod
    def remove_sample_files(model_path: pathlib.Path, seed_path: pathlib.Path) -> None:
        """Remove the sample files generated."""
        dbtMetricFlowTutorialHelper.remove_files(model_path)
        dbtMetricFlowTutorialHelper.remove_files(seed_path)

    @staticmethod
    def remove_files(path: pathlib.Path) -> None:
        """Remove the sample files generated."""
        if path.exists():
            shutil.rmtree(path)

    @staticmethod
    def check_if_path_exists(paths: Sequence[pathlib.Path]) -> bool:
        """Check if the given set of paths already exists, return True if any of the paths exists."""
        return any(p.exists() for p in paths)

    @staticmethod
    def generate_dbt_project(project_path: pathlib.Path) -> None:
        """Generate a sample dbt project using a self-contained DuckDB instance into the given directory."""
        sample_project_path = pathlib.Path(__file__).parent / dbtMetricFlowTutorialHelper.SAMPLE_DBT_PROJECT_DIRECTORY
        shutil.copytree(src=sample_project_path, dst=project_path)
