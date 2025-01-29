from __future__ import annotations

import importlib.metadata
import logging
import os
import pathlib
import shutil
import textwrap
from string import Template
from typing import Sequence

import click
import packaging.version
from halo import Halo

from dbt_metricflow.cli.cli_configuration import CLIConfiguration
from dbt_metricflow.cli.utils import (
    dbt_project_file_exists,
)

logger = logging.getLogger(__name__)


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

    @staticmethod
    def run_tutorial(cfg: CLIConfiguration, message: bool, clean: bool, yes: bool) -> None:
        """Run user through a CLI tutorial.

        See the associated Click command for details on the arguments.
        """
        # Needed to handle the backslash outside f-string
        complex_query = (
            """mf query \\
                    --metrics transactions,transaction_usd_na \\
                    --group-by metric_time,transaction__is_large \\
                    --order metric_time \\
                    --start-time 2022-03-20 --end-time 2022-04-01
            """
        ).rstrip()
        dbt_core_package_name = "dbt-core"
        dbt_core_version = None
        try:
            dbt_core_version = packaging.version.parse(importlib.metadata.version(dbt_core_package_name))
        except Exception as e:
            click.secho(
                textwrap.dedent(
                    f"""
                        Unable to determine the version of package {dbt_core_package_name!r}:
                            {str(e).splitlines()[0]}
                        Displayed links to dbt docs may not reflect the correct version for your installed version of {dbt_core_package_name!r}
                        """
                ),
                fg="yellow",
            )
        time_spine_docs_link = "https://docs.getdbt.com/docs/build/metricflow-time-spine"
        if dbt_core_version is not None:
            time_spine_docs_link = time_spine_docs_link + f"?version={dbt_core_version.major}.{dbt_core_version.minor}"
        help_msg = textwrap.dedent(
            f"""\
            🤓 {click.style("Please run the following steps:", bold=True)}

            1.  If you're using the tutorial-generated dbt project, switch to the root directory of the project.
            2.  Otherwise, if you're using your own dbt project:
                * Verify that your adapter credentials are correct in `profiles.yml`.
                * Add a time spine model to the model directory. See (try <CTRL>+Left Click on the link):
                  {click.style(time_spine_docs_link, fg="blue", bold=True)}
            3.  Run {click.style("`dbt seed`", bold=True)} and check that the steps related to countries, transactions, customers are passing.
            4.  Run {click.style("`dbt build`", bold=True)} to produce the model tables.
            4.  Try validating your data model: {click.style("`mf validate-configs`", bold=True)}
            5.  Check out your metrics: {click.style("`mf list metrics`", bold=True)}
            6.  Check out dimensions for your metric {click.style("`mf list dimensions --metrics transactions`", bold=True)}
            7.  Query your first metric:
                    {click.style("mf query --metrics transactions --group-by metric_time --order metric_time", bold=True)}
            8.  Show the SQL MetricFlow generates:
                    {click.style("mf query --metrics transactions --group-by metric_time --order metric_time --explain", bold=True)}
            9.  Visualize the plan:
                    {click.style("mf query --metrics transactions --group-by metric_time --order metric_time --explain --display-plans", bold=True)}
                * This only works if you have graphviz installed - see README.
            10.  Add another dimension:
                    {click.style("mf query --metrics transactions --group-by metric_time,customer__customer_country --order metric_time", bold=True)}
            11.  Add a coarser time granularity:
                    {click.style("mf query --metrics transactions --group-by metric_time__week --order metric_time__week", bold=True)}
            12. Try a more complicated query:
                    {click.style(complex_query, bold=True)}
            13. When you're done with the tutorial, run mf tutorial --clean to delete sample models and seeds.
                * If a sample project was created, it wil remain.
            """
        )

        if message:
            click.echo(help_msg)
            exit()

        current_directory = pathlib.Path.cwd()
        project_path = current_directory

        if not dbt_project_file_exists():
            tutorial_project_name = "mf_tutorial_project"

            sample_dbt_project_path = (current_directory / tutorial_project_name).absolute()
            click.secho(
                "Unable to detect a dbt project. Please run `mf tutorial` from the root directory of your dbt project.",
                fg="yellow",
            )
            yes or click.confirm(
                textwrap.dedent(
                    f"""\

                    Alternatively, this tutorial can create a sample dbt project with a `profiles.yml` configured to
                    use DuckDB. This will allow you to run the tutorial as a self-contained experience. The sample project
                    will be created at:

                        {click.style(str(sample_dbt_project_path), bold=True)}

                    Do you want to create the sample project now?
                    """
                ).rstrip(),
                abort=True,
            )

            dbtMetricFlowTutorialHelper._check_duckdb_package_installed_for_sample_project(yes)

            if dbtMetricFlowTutorialHelper.check_if_path_exists([sample_dbt_project_path]):
                yes or click.confirm(
                    click.style(
                        textwrap.dedent(
                            f"""\

                            The path {str(sample_dbt_project_path)!r} already exists.
                            Do you want to overwrite it?
                            """
                        ).rstrip(),
                        fg="yellow",
                    ),
                    abort=True,
                )
                dbtMetricFlowTutorialHelper.remove_files(sample_dbt_project_path)
            spinner = Halo(text=f"Generating {repr(tutorial_project_name)} files...", spinner="dots")
            spinner.start()

            dbtMetricFlowTutorialHelper.generate_dbt_project(sample_dbt_project_path)
            spinner.succeed("📦 Sample dbt project has been generated.")
            click.secho(
                textwrap.dedent(
                    """\

                    Before running the steps in the tutorial, be sure to switch to the sample project directory.
                    """
                ),
                bold=True,
            )
            os.chdir(sample_dbt_project_path.as_posix())
            project_path = sample_dbt_project_path

        click.echo(f"Using the project in {str(project_path)!r}\n")
        cfg.setup()

        # TODO: Health checks

        # Load the metadata from dbt project
        try:
            dbt_project_metadata = cfg.dbt_project_metadata
            dbt_paths = dbt_project_metadata.dbt_paths
            model_path = pathlib.Path(dbt_paths.model_paths[0]) / "sample_model"
            seed_path = pathlib.Path(dbt_paths.seed_paths[0]) / "sample_seed"
            manifest_path = pathlib.Path(dbt_paths.target_path) / "semantic_manifest.json"
        except Exception as e:
            click.echo(f"Unable to parse path metadata from dbt project.\nERROR: {str(e)}")
            exit(1)

        # Remove sample files from dbt project
        if clean:
            yes or click.confirm("Would you like to remove all the sample files?", abort=True)
            spinner = Halo(text="Removing sample files...", spinner="dots")
            spinner.start()
            try:
                dbtMetricFlowTutorialHelper.remove_sample_files(model_path=model_path, seed_path=seed_path)
                spinner.succeed("🗑️ Sample files has been removed.")
                exit()
            except Exception as e:
                spinner.fail(f"❌ Unable to remove sample files.\nERROR: {str(e)}")
                exit(1)

        # TODO: Why a JSON file for the manifest?
        click.echo(
            textwrap.dedent(
                f"""\
                To begin building and querying metrics, you must define semantic models and
                metric configuration files in your dbt project. dbt will use these files to generate a
                semantic manifest artifact, which MetricFlow will use to create a semantic graph for querying.
                As part of this tutorial, we will generate the following files to help you get started:

                📜 Model Files
                    -> {model_path.absolute().as_posix()}
                🌱 Seed Files
                    -> {seed_path.absolute().as_posix()}
                ✅ Semantic Manifest JSON File
                    -> {manifest_path.absolute().as_posix()}
                """
            )
        )
        yes or click.confirm("Continue and generate the files?", abort=True)

        # Generate sample files into dbt project
        if dbtMetricFlowTutorialHelper.check_if_path_exists([model_path, seed_path]):
            yes or click.confirm(
                click.style(
                    "There are existing files in the paths above, would you like to overwrite them?", fg="yellow"
                ),
                abort=True,
            )
            dbtMetricFlowTutorialHelper.remove_sample_files(model_path=model_path, seed_path=seed_path)

        spinner = Halo(text="Generating sample files...", spinner="dots")
        spinner.start()
        dbtMetricFlowTutorialHelper.generate_model_files(
            model_path=model_path, profile_schema=dbt_project_metadata.schema
        )
        dbtMetricFlowTutorialHelper.generate_seed_files(seed_path=seed_path)
        dbtMetricFlowTutorialHelper.generate_semantic_manifest_file(manifest_path=manifest_path)

        spinner.succeed("📜 Sample files has been generated.")

        click.echo("\n" + help_msg)
        click.echo("💡 Run `mf tutorial --message` to see this message again without executing everything else")
        exit()

    @staticmethod
    def _check_duckdb_package_installed_for_sample_project(yes: bool) -> None:
        """Check if the DuckDB adapter package is installed and prompt user to install it if not.

        If `yes` is set, the prompt to exit if the package is not installed will be skipped.
        """
        click.echo(
            textwrap.dedent(
                """\

                Since the sample project uses DuckDB, the `dbt-metricflow[dbt-duckdb]` package must be installed beforehand.
                """
            ).rstrip(),
        )

        duckdb_adapter_package_name = "dbt-duckdb"
        dbt_duckdb_package_version = None
        try:
            dbt_duckdb_package_version = importlib.metadata.version(duckdb_adapter_package_name)
        except importlib.metadata.PackageNotFoundError:
            pass

        if dbt_duckdb_package_version is not None:
            click.secho(
                f"* Detected installed package {duckdb_adapter_package_name!r} {dbt_duckdb_package_version}",
                bold=True,
                fg="green",
            )
            return

        click.secho("* Did not detect package is installed", bold=True, fg="red")

        if yes:
            return

        exit_tutorial = click.confirm(
            textwrap.dedent(
                """\

                As the package was not detected as installed, it's recommended to install the package first before
                generating the sample project. e.g. `pip install 'dbt-metricflow[dbt-duckdb]'`. Otherwise, there may be
                errors that prevent you from generating the sample project and running the tutorial.

                Do you want to exit the tutorial so that you can install the package?
                """
            ).rstrip(),
        )
        if exit_tutorial:
            exit(0)
