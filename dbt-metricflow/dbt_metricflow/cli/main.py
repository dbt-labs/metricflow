from __future__ import annotations

import csv as csv_module
import datetime as dt
import logging
import pathlib
import signal
import sys
import tempfile
import textwrap
import time
import warnings
from importlib.metadata import version as pkg_version
from typing import Callable, List, Optional, Sequence

import click
import jinja2
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.validations.semantic_manifest_validator import SemanticManifestValidator
from dbt_semantic_interfaces.validations.validator_helpers import SemanticManifestValidationResults
from halo import Halo
from metricflow_semantics.dag.dag_visualization import display_dag_as_svg
from update_checker import UpdateChecker

import dbt_metricflow.cli.custom_click_types as click_custom
from dbt_metricflow.cli import PACKAGE_NAME
from dbt_metricflow.cli.cli_context import CLIContext
from dbt_metricflow.cli.constants import DEFAULT_RESULT_DECIMAL_PLACES, MAX_LIST_OBJECT_ELEMENTS
from dbt_metricflow.cli.dbt_connectors.dbt_config_accessor import dbtArtifacts
from dbt_metricflow.cli.tutorial import (
    dbtMetricFlowTutorialHelper,
)
from dbt_metricflow.cli.utils import (
    dbt_project_file_exists,
    error_if_not_in_dbt_project,
    exception_handler,
    query_options,
    start_end_time_options,
)
from metricflow.engine.metricflow_engine import MetricFlowExplainResult, MetricFlowQueryRequest, MetricFlowQueryResult
from metricflow.telemetry.models import TelemetryLevel
from metricflow.telemetry.reporter import TelemetryReporter, log_call
from metricflow.validation.data_warehouse_model_validator import DataWarehouseModelValidator

logger = logging.getLogger(__name__)

pass_config = click.make_pass_decorator(CLIContext, ensure=True)
_telemetry_reporter = TelemetryReporter(report_levels_higher_or_equal_to=TelemetryLevel.USAGE)
_telemetry_reporter.add_python_log_handler()


@click.group()
@click.option("-v", "--verbose", is_flag=True)
@click.version_option()
@error_if_not_in_dbt_project
@pass_config
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def cli(cfg: CLIContext, verbose: bool) -> None:  # noqa: D103
    # Some HTTP logging callback somewhere is failing to close its SSL connections correctly.
    # For now, filter those warnings so they don't pop up in CLI stderr
    # note - this should be addressed as adapter connection issues might produce these as well
    warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")

    cfg.verbose = verbose

    checker = UpdateChecker()
    result = checker.check(PACKAGE_NAME, pkg_version(PACKAGE_NAME))
    # result is None when an update was not found or a failure occurred
    if result:
        click.secho(
            "‼️ Warning: A new version of the MetricFlow CLI is available.",
            bold=True,
            fg="red",
        )

        click.echo(
            f"💡 Please update to version {result.available_version}, released {result.release_date} by running:\n"
            f"\t$ pip install --upgrade {PACKAGE_NAME}\n",
        )

    # Cancel queries submitted to the DW if the user precess CTRL + c / process is terminated.
    # Note: docs unclear on the type for the 'frame' argument.
    def exit_signal_handler(signal_type: int, frame) -> None:  # type: ignore
        if signal_type == signal.SIGINT:
            click.echo("Got SIGINT")
        elif signal_type == signal.SIGTERM:
            click.echo("Got SIGTERM")
        else:
            # Shouldn't happen since this should ony be registered for SIGINT / SIGTERM.
            click.echo(f"Got unhandled signal {signal_type}")
            return

        try:
            # Note: we may wish to add support for canceling all queries if zombie queries are a problem
            logger.info("Closing client connections")
            cfg.sql_client.close()
        finally:
            sys.exit(-1)

    signal.signal(signal.SIGINT, exit_signal_handler)
    signal.signal(signal.SIGTERM, exit_signal_handler)


@cli.command()
@click.option("-m", "--msg", is_flag=True, help="Output the final steps dialogue")
# @click.option("--skip-dw", is_flag=True, help="Skip the data warehouse health checks") # TODO: re-enable this
@click.option("--clean", is_flag=True, help="Remove sample model files.")
@pass_config
@click.pass_context
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def tutorial(ctx: click.core.Context, cfg: CLIContext, msg: bool, clean: bool) -> None:
    """Run user through a tutorial."""
    help_msg = textwrap.dedent(
        """\
        🤓 Please run the following steps,
            1.  Verify that your adapter credentials are correct in `profiles.yml`
            2.  Add time spine model to the models directory (https://docs.getdbt.com/docs/build/metricflow-time-spine)
            3.  Run `dbt seed`, check to see that the steps related to countries, transactions, customers are passing.
            4.  Try validating your data model: `mf validate-configs`
            5.  Check out your metrics: `mf list metrics`
            6.  Check out dimensions for your metric `mf list dimensions --metrics transactions`
            7.  Query your first metric: `mf query --metrics transactions --group-by metric_time --order metric_time`
            8.  Show the SQL MetricFlow generates:
                `mf query --metrics transactions --group-by metric_time --order metric_time --explain`
            9.  Visualize the plan:
                `mf query --metrics transactions --group-by metric_time --order metric_time --explain --display-plans`
                * This only works if you have graphviz installed - see README.
            10.  Add another dimension:
                `mf query --metrics transactions --group-by metric_time,customer__customer_country --order metric_time`
            11.  Add a coarser time granularity:
                `mf query --metrics transactions --group-by metric_time__week --order metric_time__week`
            12. Try a more complicated query: mf query --metrics transactions,transaction_usd_na --group-by metric_time,is_large --order metric_time --start-time 2022-03-20 --end-time 2022-04-01.
            13. When you're done with the tutorial, run mf tutorial --clean to delete sample models and seeds.
        """
    )

    if msg:
        click.echo(help_msg)
        exit()

    if not dbt_project_file_exists():
        click.echo(
            "Unable to detect dbt project. Please ensure that your current working directory is at the root of the dbt project."
        )
        exit()

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
        click.confirm("Would you like to remove all the sample files?", abort=True)
        spinner = Halo(text="Removing sample files...", spinner="dots")
        spinner.start()
        try:
            dbtMetricFlowTutorialHelper.remove_sample_files(model_path=model_path, seed_path=seed_path)
            spinner.succeed("🗑️ Sample files has been removed.")
            exit()
        except Exception as e:
            spinner.fail(f"❌ Unable to remove sample files.\nERROR: {str(e)}")
            exit(1)

    click.echo(
        textwrap.dedent(
            f"""\
        To begin building and querying metrics, you must define semantic models and
        metric configuration files in your dbt project. dbt will use these files to generate a
        semantic manifest artifact, which MetricFlow will use to create a semantic graph for querying.
        As part of this tutorial, we will generate the following files to help you get started:

        📜 model files -> {model_path.absolute().as_posix()}
        🌱 seed files -> {seed_path.absolute().as_posix()}
        ✅ semantic manifest json file -> {manifest_path.absolute().as_posix()}
        """
        )
    )
    click.confirm("Continue and generate the files?", abort=True)

    # Generate sample files into dbt project
    if dbtMetricFlowTutorialHelper.check_if_path_exists([model_path, seed_path]):
        click.confirm("There are existing files in the paths above, would you like to overwrite them?", abort=True)
        dbtMetricFlowTutorialHelper.remove_sample_files(model_path=model_path, seed_path=seed_path)

    spinner = Halo(text="Generating sample files...", spinner="dots")
    spinner.start()
    dbtMetricFlowTutorialHelper.generate_model_files(model_path=model_path, profile_schema=dbt_project_metadata.schema)
    dbtMetricFlowTutorialHelper.generate_seed_files(seed_path=seed_path)
    dbtMetricFlowTutorialHelper.generate_semantic_manifest_file(manifest_path=manifest_path)

    spinner.succeed("📜 Sample files has been generated.")

    click.echo(help_msg)
    click.echo("💡 Run `mf tutorial --msg` to see this message again without executing everything else")
    exit()


@cli.command()
@query_options
@click.option(
    "--csv",
    type=click.File("w"),
    required=False,
    help="Provide filepath for data_table output to csv",
)
@click.option(
    "--explain",
    is_flag=True,
    required=False,
    default=False,
    help="In the query output, show the query that was executed against the data warehouse",
)
@click.option(
    "--show-dataflow-plan",
    is_flag=True,
    required=False,
    default=False,
    help="Display dataflow plan in explain output",
)
@click.option(
    "--display-plans",
    is_flag=True,
    required=False,
    help="Display plans (e.g. metric dataflow) in the browser",
)
@click.option(
    "--decimals",
    required=False,
    default=2,
    help="Choose the number of decimal places to round for the numerical values",
)
@click.option(
    "--show-sql-descriptions",
    is_flag=True,
    default=False,
    help="Shows inline descriptions of nodes in displayed SQL",
)
@click.option(
    "--saved-query",
    required=False,
    help="Specify the name of the saved query to use for applicable parameters",
)
@pass_config
@exception_handler
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def query(
    cfg: CLIContext,
    metrics: Optional[Sequence[str]] = None,
    group_by: Optional[Sequence[str]] = None,
    where: Optional[str] = None,
    start_time: Optional[dt.datetime] = None,
    end_time: Optional[dt.datetime] = None,
    order: Optional[List[str]] = None,
    limit: Optional[int] = None,
    csv: Optional[click.utils.LazyFile] = None,
    explain: bool = False,
    show_dataflow_plan: bool = False,
    display_plans: bool = False,
    decimals: int = DEFAULT_RESULT_DECIMAL_PLACES,
    show_sql_descriptions: bool = False,
    saved_query: Optional[str] = None,
) -> None:
    """Create a new query with MetricFlow and assembles a MetricFlowQueryResult."""
    start = time.time()
    spinner = Halo(text="Initiating query…", spinner="dots")
    spinner.start()
    mf_request = MetricFlowQueryRequest.create_with_random_request_id(
        saved_query_name=saved_query,
        metric_names=metrics,
        group_by_names=group_by,
        limit=limit,
        time_constraint_start=start_time,
        time_constraint_end=end_time,
        where_constraint=where,
        order_by_names=order,
    )

    explain_result: Optional[MetricFlowExplainResult] = None
    query_result: Optional[MetricFlowQueryResult] = None

    if explain:
        explain_result = cfg.mf.explain(mf_request=mf_request)
    else:
        query_result = cfg.mf.query(mf_request=mf_request)

    spinner.succeed(f"Success 🦄 - query completed after {time.time() - start:.2f} seconds")

    if explain:
        assert explain_result
        sql = (
            explain_result.rendered_sql_without_descriptions.sql_query
            if not show_sql_descriptions
            else explain_result.rendered_sql.sql_query
        )
        if show_dataflow_plan:
            click.echo("🔎 Generated Dataflow Plan + SQL (remove --explain to see data):")
            click.echo(
                textwrap.indent(
                    jinja2.Template(
                        textwrap.dedent(
                            """\
                            Metric Dataflow Plan:
                                {{ plan_text | indent(4) }}
                            """
                        ),
                        undefined=jinja2.StrictUndefined,
                    ).render(plan_text=explain_result.dataflow_plan.structure_text()),
                    prefix="-- ",
                )
            )
            click.echo("")
        else:
            click.echo(
                "🔎 SQL (remove --explain to see data or add --show-dataflow-plan to see the generated dataflow plan):"
            )
        click.echo(sql)
        if display_plans:
            click.echo("Creating temporary directory for storing visualization output.")
            temp_path = tempfile.mkdtemp()
            svg_path = display_dag_as_svg(explain_result.dataflow_plan, temp_path)
            click.echo("")
            click.echo(f"Plan SVG saved to: {svg_path}")
        exit()

    assert query_result
    df = query_result.result_df
    # Show the data if returned successfully
    if df is not None:
        if df.row_count == 0:
            click.echo("🕳 Successful MQL query returned an empty result set.")
        elif csv is not None:
            # csv is a LazyFile that is file-like that works in this case.
            csv_writer = csv_module.writer(csv)
            csv_writer.writerow(df.column_names)
            for row in df.rows:
                csv_writer.writerow(row)
            click.echo(f"🖨 Successfully written query output to {csv.name}")
        else:
            click.echo(df.text_format(decimals))
        if display_plans:
            temp_path = tempfile.mkdtemp()
            svg_path = display_dag_as_svg(query_result.dataflow_plan, temp_path)
            click.echo(f"Plan SVG saved to: {svg_path}")


@cli.group()
@pass_config
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def list(cfg: CLIContext) -> None:
    """Retrieve metadata values about metrics/dimensions/entities/dimension values."""


@list.command()
@click.option("--search", required=False, type=str, help="Filter available metrics by this search term")
@click.option(
    "--show-all-dimensions", is_flag=True, default=False, help="Show all dimensions associated with a metric."
)
@pass_config
@exception_handler
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def metrics(cfg: CLIContext, show_all_dimensions: bool = False, search: Optional[str] = None) -> None:
    """List the metrics with their available dimensions.

    Automatically truncates long lists of dimensions, pass --show-all-dims to see all.
    """
    spinner = Halo(text="🔍 Looking for all available metrics...", spinner="dots")
    spinner.start()

    metrics = cfg.mf.list_metrics()

    if not metrics:
        spinner.fail("List of metrics unavailable.")

    filter_msg = ""
    if search is not None:
        num_metrics = len(metrics)
        metrics = [m for m in metrics if search.lower() in m.name.lower()]
        filter_msg = f" matching `{search}`, of a total of {num_metrics} available"

    spinner.succeed(f"🌱 We've found {len(metrics)} metrics{filter_msg}.")
    click.echo('The list below shows metrics in the format of "metric_name: list of available dimensions"')
    num_dims_to_show = MAX_LIST_OBJECT_ELEMENTS
    for m in metrics:
        # sort dimensions by whether they're local first(if / then global else local) then the dim name
        dimensions = sorted([dimension.granularity_free_qualified_name for dimension in m.dimensions])
        if show_all_dimensions:
            num_dims_to_show = len(dimensions)
        click.echo(
            f"• {click.style(m.name, bold=True, fg='green')}: {', '.join(dimensions[:num_dims_to_show])}"
            + (f" and {len(dimensions) - num_dims_to_show} more" if len(dimensions) > num_dims_to_show else "")
        )


@list.command()
@click.option(
    "--metrics",
    type=click_custom.SequenceParamType(min_length=1),
    default="",
    help="List dimensions by given metrics (intersection). Ex. --metrics bookings,messages",
)
@pass_config
@exception_handler
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def dimensions(cfg: CLIContext, metrics: List[str]) -> None:
    """List all unique dimensions."""
    spinner = Halo(
        text="🔍 Looking for all available dimensions...",
        spinner="dots",
    )
    spinner.start()

    dimensions = cfg.mf.simple_dimensions_for_metrics(metrics)
    if not dimensions:
        spinner.fail("List of dimensions unavailable.")

    spinner.succeed(f"🌱 We've found {len(dimensions)} common dimensions for metrics {metrics}.")
    for dimension in dimensions:
        click.echo(f"• {click.style(dimension.granularity_free_qualified_name, bold=True, fg='green')}")


@list.command()
@click.option(
    "--metrics",
    type=click_custom.SequenceParamType(min_length=1),
    default="",
    help="List entities by given metrics (intersection). Ex. --metrics bookings,messages",
)
@pass_config
@exception_handler
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def entities(cfg: CLIContext, metrics: List[str]) -> None:
    """List all unique entities."""
    spinner = Halo(
        text="🔍 Looking for all available entities...",
        spinner="dots",
    )
    spinner.start()

    entities = cfg.mf.entities_for_metrics(metrics)
    if not entities:
        spinner.fail("List of entities unavailable.")

    spinner.succeed(f"🌱 We've found {len(entities)} common entities for metrics {metrics}.")
    for entity in entities:
        click.echo(f"• {click.style(entity.name, bold=True, fg='green')}")


@cli.command()
@pass_config
@exception_handler
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def health_checks(cfg: CLIContext) -> None:
    """Performs a health check against the DW provided in the configs."""
    spinner = Halo(
        text="🏥 Running health checks against your data warehouse... (This should not take longer than 30s for a successful connection)",
        spinner="dots",
    )
    spinner.start()
    res = cfg.run_health_checks()
    spinner.succeed("Health checks completed.")
    for test in res:
        test_res = res[test]
        if test_res["status"] != "SUCCESS":
            click.echo(f"• ❌ {click.style(test, bold=True, fg=('red'))}:  Failed with - {test_res['error_message']}.")
        else:
            click.echo(f"• ✅ {click.style(test, bold=True, fg=('green'))}: Success!")


@list.command()
@click.option("--dimension", required=True, type=str, help="Dimension to query values from")
@click.option(
    "--metrics",
    required=True,
    type=click_custom.SequenceParamType(min_length=1),
    help="Metrics that are associated with the dimension",
)
@start_end_time_options
@pass_config
@exception_handler
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def dimension_values(
    cfg: CLIContext,
    metrics: List[str],
    dimension: str,
    start_time: Optional[dt.datetime] = None,
    end_time: Optional[dt.datetime] = None,
) -> None:
    """List all dimension values with the corresponding metrics."""
    spinner = Halo(
        text=f"🔍 Retrieving dimension values for dimension '{dimension}' of metrics '{', '.join(metrics)}'...",
        spinner="dots",
    )
    spinner.start()

    dim_vals: Optional[List[str]] = None

    try:
        dim_vals = cfg.mf.get_dimension_values(
            metric_names=metrics,
            get_group_by_values=dimension,
            time_constraint_start=start_time,
            time_constraint_end=end_time,
        )
    except Exception as e:
        spinner.fail()
        click.echo(
            textwrap.dedent(
                f"""\
                ❌ Failed to query dimension values for dimension {dimension} of metrics {', '.join(metrics)}.
                    ERROR: {str(e)}
                """
            )
        )
        exit(1)

    assert dim_vals
    spinner.succeed(
        f"🌱 We've found {len(dim_vals)} dimension values for dimension {dimension} of metrics {', '.join(metrics)}."
    )
    for dim_val in dim_vals:
        click.echo(f"• {click.style(dim_val, bold=True, fg='green')}")


def _print_issues(
    issues: SemanticManifestValidationResults, show_non_blocking: bool = False, verbose: bool = False
) -> None:
    for issue in issues.errors:
        print(f"• {issue.as_cli_formatted_str(verbose=verbose)}")
    if show_non_blocking:
        for issue in issues.future_errors:
            print(f"• {issue.as_cli_formatted_str(verbose=verbose)}")
        for issue in issues.warnings:
            print(f"• {issue.as_cli_formatted_str(verbose=verbose)}")


def _run_dw_validations(
    validation_func: Callable[[SemanticManifest, Optional[int]], SemanticManifestValidationResults],
    validation_type: str,
    manifest: SemanticManifest,
    timeout: Optional[int],
) -> SemanticManifestValidationResults:
    """Helper handles the calling of data warehouse issue generating functions."""
    spinner = Halo(text=f"Validating {validation_type} against data warehouse...", spinner="dots")
    spinner.start()

    results = validation_func(manifest, timeout)
    if not results.has_blocking_issues:
        spinner.succeed(f"🎉 Successfully validated {validation_type} against data warehouse ({results.summary()})")
    else:
        spinner.fail(
            f"Breaking issues found when validating {validation_type} against data warehouse ({results.summary()})"
        )
    return results


def _data_warehouse_validations_runner(
    dw_validator: DataWarehouseModelValidator, manifest: SemanticManifest, timeout: Optional[int]
) -> SemanticManifestValidationResults:
    """Helper which calls the individual data warehouse validations to run and prints collected issues."""
    semantic_model_results = _run_dw_validations(
        dw_validator.validate_semantic_models, manifest=manifest, validation_type="semantic models", timeout=timeout
    )
    dimension_results = _run_dw_validations(
        dw_validator.validate_dimensions, manifest=manifest, validation_type="dimensions", timeout=timeout
    )
    entity_results = _run_dw_validations(
        dw_validator.validate_entities, manifest=manifest, validation_type="entities", timeout=timeout
    )
    measure_results = _run_dw_validations(
        dw_validator.validate_measures, manifest=manifest, validation_type="measures", timeout=timeout
    )
    metric_results = _run_dw_validations(
        dw_validator.validate_metrics, manifest=manifest, validation_type="metrics", timeout=timeout
    )

    return SemanticManifestValidationResults.merge(
        [semantic_model_results, dimension_results, entity_results, measure_results, metric_results]
    )


@cli.command()
@click.option(
    "--dw-timeout", required=False, type=int, help="Optional timeout for data warehouse validation steps. Default None."
)
@click.option(
    "--skip-dw",
    is_flag=True,
    default=False,
    help="If specified, skips the data warehouse validations",
)
@click.option("--show-all", is_flag=True, default=False, help="If specified, prints warnings and future-errors")
@click.option(
    "--verbose-issues", is_flag=True, default=False, help="If specified, prints any extra details issues might have"
)
@click.option(
    "--semantic-validation-workers",
    required=False,
    type=int,
    default=1,
    help="Optional. Uses the number of workers specified to run the semantic validations. Should only be used for exceptionally large configs",
)
@pass_config
@exception_handler
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def validate_configs(
    cfg: CLIContext,
    dw_timeout: Optional[int] = None,
    skip_dw: bool = False,
    show_all: bool = False,
    verbose_issues: bool = False,
    semantic_validation_workers: int = 1,
) -> None:
    """Perform validations against the defined model configurations."""
    cfg.verbose = True

    if not show_all:
        print("(To see warnings and future-errors, run again with flag `--show-all`)")

    # Parsing Validation
    parsing_spinner = Halo(text="Building manifest from dbt project root", spinner="dots")
    parsing_spinner.start()
    project_root = pathlib.Path.cwd()

    try:
        semantic_manifest = dbtArtifacts.build_semantic_manifest_from_dbt_project_root(project_root=project_root)
        parsing_spinner.succeed("🎉 Successfully parsed manifest from dbt project")
    except Exception as e:
        parsing_spinner.fail(f"Exception found when parsing manifest from dbt project ({str(e)})")
        exit(1)

    # Semantic validation
    semantic_spinner = Halo(text="Validating semantics of built manifest", spinner="dots")
    semantic_spinner.start()
    model_issues = SemanticManifestValidator[SemanticManifest](
        max_workers=semantic_validation_workers
    ).validate_semantic_manifest(semantic_manifest)

    if not model_issues.has_blocking_issues:
        semantic_spinner.succeed(f"🎉 Successfully validated the semantics of built manifest ({model_issues.summary()})")
    else:
        semantic_spinner.fail(
            f"Breaking issues found when checking semantics of built manifest ({model_issues.summary()})"
        )
        _print_issues(model_issues, show_non_blocking=show_all, verbose=verbose_issues)
        exit(1)

    dw_results = SemanticManifestValidationResults()
    if not skip_dw:
        # fetch dbt adapters. This rebuilds the manifest again, but whatever.
        dw_validator = DataWarehouseModelValidator(sql_client=cfg.sql_client)
        dw_results = _data_warehouse_validations_runner(
            dw_validator=dw_validator, manifest=semantic_manifest, timeout=dw_timeout
        )

    merged_results = SemanticManifestValidationResults.merge([model_issues, dw_results])
    _print_issues(merged_results, show_non_blocking=show_all, verbose=verbose_issues)
    if merged_results.has_blocking_issues:
        exit(1)


if __name__ == "__main__":
    cli()
