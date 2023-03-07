import contextlib
import logging
import signal
import sys

import click
import datetime as dt
import jinja2
import os
import pandas as pd
import pathlib
import textwrap
import time

from halo import Halo
from importlib.metadata import version as pkg_version
from packaging.version import parse
from typing import Callable, Iterator, List, Optional
from update_checker import UpdateChecker

from metricflow.cli import PACKAGE_NAME
from metricflow.cli.constants import DEFAULT_RESULT_DECIMAL_PLACES, MAX_LIST_OBJECT_ELEMENTS
from metricflow.cli.cli_context import CLIContext
import metricflow.cli.custom_click_types as click_custom
from metricflow.cli.utils import (
    exception_handler,
    generate_duckdb_demo_keys,
    get_data_warehouse_config_link,
    query_options,
    start_end_time_options,
    MF_BIGQUERY_KEYS,
    MF_CONFIG_KEYS,
    MF_REDSHIFT_KEYS,
    MF_SNOWFLAKE_KEYS,
    MF_POSTGRESQL_KEYS,
    MF_DATABRICKS_KEYS,
)
from metricflow.configuration.config_builder import YamlTemplateBuilder
from metricflow.dataflow.dataflow_plan_to_text import dataflow_plan_as_text
from metricflow.engine.metricflow_engine import MetricFlowQueryRequest, MetricFlowExplainResult, MetricFlowQueryResult
from metricflow.model.data_warehouse_model_validator import DataWarehouseModelValidator
from dbt.semantic.validations.model_validator import ModelValidator
from dbt.semantic.validations.validator_helpers import ModelValidationResults
from dbt.semantic.user_configured_model import UserConfiguredModel
from metricflow.protocols.sql_client import SqlEngine
from metricflow.model.parsing.dbt_dir_to_model import get_dbt_user_configured_model
from metricflow.sql_clients.common_client import SqlDialect
from metricflow.telemetry.models import TelemetryLevel
from metricflow.telemetry.reporter import TelemetryReporter, log_call
from metricflow.dag.dag_visualization import display_dag_as_svg

logger = logging.getLogger(__name__)

pass_config = click.make_pass_decorator(CLIContext, ensure=True)
_telemetry_reporter = TelemetryReporter(report_levels_higher_or_equal_to=TelemetryLevel.USAGE)
_telemetry_reporter.add_python_log_handler()
_telemetry_reporter.add_rudderstack_handler()


@click.group()
@click.option("-v", "--verbose", is_flag=True)
@pass_config
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def cli(cfg: CLIContext, verbose: bool) -> None:  # noqa: D
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
            if cfg.sql_client.sql_engine_attributes.cancel_submitted_queries_supported:
                logger.info("Cancelling submitted queries")
                cfg.sql_client.cancel_submitted_queries()
                cfg.sql_client.close()
        finally:
            sys.exit(-1)

    signal.signal(signal.SIGINT, exit_signal_handler)
    signal.signal(signal.SIGTERM, exit_signal_handler)


@cli.command()
def version() -> None:
    """Print the current version of the MetricFlow CLI."""
    click.echo(pkg_version(PACKAGE_NAME))


@cli.command()
@query_options
@click.option(
    "--as-table",
    required=False,
    type=str,
    help="Output the data to a specified SQL table in the form of '<schema>.<table>'",
)
@click.option(
    "--csv",
    type=click.File("wb"),
    required=False,
    help="Provide filepath for dataframe output to csv",
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
@pass_config
@exception_handler
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def query(
    cfg: CLIContext,
    metrics: List[str],
    dimensions: List[str] = [],
    where: Optional[str] = None,
    start_time: Optional[dt.datetime] = None,
    end_time: Optional[dt.datetime] = None,
    order: Optional[List[str]] = None,
    limit: Optional[int] = None,
    as_table: Optional[str] = None,
    csv: Optional[click.utils.LazyFile] = None,
    explain: bool = False,
    show_dataflow_plan: bool = False,
    display_plans: bool = False,
    decimals: int = DEFAULT_RESULT_DECIMAL_PLACES,
    show_sql_descriptions: bool = False,
) -> None:
    """Create a new query with MetricFlow and assembles a MetricFlowQueryResult."""
    start = time.time()

    mf_request = MetricFlowQueryRequest.create_with_random_request_id(
        metric_names=metrics,
        group_by_names=dimensions,
        limit=limit,
        time_constraint_start=start_time,
        time_constraint_end=end_time,
        where_constraint=where,
        order_by_names=order,
        output_table=as_table,
    )

    explain_result: Optional[MetricFlowExplainResult] = None
    query_result: Optional[MetricFlowQueryResult] = None

    if explain:
        explain_result = cfg.mf.explain(mf_request=mf_request)
    else:
        query_result = cfg.mf.query(mf_request=mf_request)

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
                    ).render(plan_text=dataflow_plan_as_text(explain_result.dataflow_plan)),
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
            svg_path = display_dag_as_svg(explain_result.dataflow_plan, cfg.config.dir_path)
            click.echo("")
            click.echo(f"Plan SVG saved to: {svg_path}")
        exit()

    assert query_result
    df = query_result.result_df
    # Show the data if returned successfully
    if df is not None:
        if df.empty:
            click.echo("🕳 Successful MQL query returned an empty result set.")
        elif csv is not None:
            # csv is a LazyFile that is file-like that works in this case.
            df.to_csv(csv, index=False)  # type: ignore
            click.echo(f"🖨 Successfully written query output to {csv.name}")
        else:
            # NOTE: remove `to_string` if no pandas dependency is < 1.1.0
            if parse(pd.__version__) >= parse("1.1.0"):
                click.echo(df.to_markdown(index=False, floatfmt=f".{decimals}f"))
            else:
                click.echo(df.to_string(index=False, float_format=lambda x: format(x, f".{decimals}f")))

        if display_plans:
            svg_path = display_dag_as_svg(query_result.dataflow_plan, cfg.config.dir_path)
            click.echo(f"Plan SVG saved to: {svg_path}")


@cli.command()
@click.option("--search", required=False, type=str, help="Filter available metrics by this search term")
@click.option("--show-all-dims", is_flag=True, default=False, help="Show all dimensions associated with a metric.")
@pass_config
@exception_handler
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def list_metrics(cfg: CLIContext, show_all_dims: bool = False, search: Optional[str] = None) -> None:
    """List the metrics with their available dimensions.

    Automatically truncates long lists of dimensions, pass --show-all-dims to see all.
    """

    metrics = cfg.mf.list_metrics()

    filter_msg = ""
    if search is not None:
        num_metrics = len(metrics)
        metrics = [m for m in metrics if search.lower() in m.name.lower()]
        filter_msg = f" matching `{search}`, of a total of {num_metrics} available"

    click.echo('The list below shows metrics in the format of "metric_name: list of available dimensions"')
    num_dims_to_show = MAX_LIST_OBJECT_ELEMENTS
    for m in metrics:
        # sort dimensions by whether they're local first(if / then global else local) then the dim name
        dimensions = sorted(map(lambda d: d.name, filter(lambda d: "/" not in d.name, m.dimensions))) + sorted(
            map(lambda d: d.name, filter(lambda d: "/" in d.name, m.dimensions))
        )
        if show_all_dims:
            num_dims_to_show = len(dimensions)
        click.echo(
            f"• {click.style(m.name, bold=True, fg='green')}: {', '.join(dimensions[:num_dims_to_show])}"
            + (f" and {len(dimensions) - num_dims_to_show} more" if len(dimensions) > num_dims_to_show else "")
        )


@cli.command()
@click.option(
    "--metrics",
    type=click_custom.SequenceParamType(),
    help="List dimensions by given metrics (intersection). Ex. --metrics bookings,messages",
)
@pass_config
@exception_handler
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def list_dimensions(cfg: CLIContext, metrics: List[str]) -> None:
    """List all unique dimensions."""

    dimensions = cfg.mf.simple_dimensions_for_metrics(metrics)

    for d in dimensions:
        click.echo(f"• {click.style(d.name, bold=True, fg='green')}")


@cli.command()
@click.option("--dimension-name", required=True, type=str, help="Dimension to query values from")
@click.option("--metric-name", required=True, type=str, help="Metric that is associated with the dimension")
@start_end_time_options
@pass_config
@exception_handler
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def get_dimension_values(
    cfg: CLIContext,
    metric_name: str,
    dimension_name: str,
    start_time: Optional[dt.datetime] = None,
    end_time: Optional[dt.datetime] = None,
) -> None:
    """List all dimension values with the corresponding metric."""

    dim_vals: Optional[List[str]] = None

    try:
        dim_vals = cfg.mf.get_dimension_values(
            metric_name=metric_name,
            get_group_by_values=dimension_name,
            time_constraint_start=start_time,
            time_constraint_end=end_time,
        )
    except Exception as e:
        click.echo(
            textwrap.dedent(
                f"""\
                ❌ Failed to query dimension values for dimension {dimension_name} of metric {metric_name}.
                    ERROR: {str(e)}
                """
            )
        )
        exit(1)

    assert dim_vals
    for dim_val in dim_vals:
        click.echo(f"• {click.style(dim_val, bold=True, fg='green')}")


def _print_issues(
    issues: ModelValidationResults, show_non_blocking: bool = False, verbose: bool = False
) -> None:  # noqa: D
    for issue in issues.errors:
        print(f"• {issue.as_cli_formatted_str(verbose=verbose)}")
    if show_non_blocking:
        for issue in issues.future_errors:
            print(f"• {issue.as_cli_formatted_str(verbose=verbose)}")
        for issue in issues.warnings:
            print(f"• {issue.as_cli_formatted_str(verbose=verbose)}")


def _run_dw_validations(
    validation_func: Callable[[UserConfiguredModel, Optional[int]], ModelValidationResults],
    validation_type: str,
    model: UserConfiguredModel,
    timeout: Optional[int],
) -> ModelValidationResults:
    """Helper handles the calling of data warehouse issue generating functions"""

    spinner = Halo(text=f"Validating {validation_type} against data warehouse...", spinner="dots")
    spinner.start()

    results = validation_func(model, timeout)
    if not results.has_blocking_issues:
        spinner.succeed(f"🎉 Successfully validated {validation_type} against data warehouse ({results.summary()})")
    else:
        spinner.fail(
            f"Breaking issues found when validating {validation_type} against data warehouse ({results.summary()})"
        )
    return results


def _data_warehouse_validations_runner(
    dw_validator: DataWarehouseModelValidator, model: UserConfiguredModel, timeout: Optional[int]
) -> ModelValidationResults:
    """Helper which calls the individual data warehouse validations to run and prints collected issues"""

    entity_results = _run_dw_validations(
        dw_validator.validate_entities, model=model, validation_type="entities", timeout=timeout
    )
    dimension_results = _run_dw_validations(
        dw_validator.validate_dimensions, model=model, validation_type="dimensions", timeout=timeout
    )
    identifier_results = _run_dw_validations(
        dw_validator.validate_identifiers, model=model, validation_type="identifiers", timeout=timeout
    )
    measure_results = _run_dw_validations(
        dw_validator.validate_measures, model=model, validation_type="measures", timeout=timeout
    )
    metric_results = _run_dw_validations(
        dw_validator.validate_metrics, model=model, validation_type="metrics", timeout=timeout
    )

    return ModelValidationResults.merge(
        [entity_results, dimension_results, identifier_results, measure_results, metric_results]
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

    user_model = get_dbt_user_configured_model(
        directory=cfg.project_dir
    )

    # Semantic validation
    semantic_result = ModelValidator(max_workers=semantic_validation_workers).validate_model(user_model)

    if semantic_result.issues.has_blocking_issues:
        _print_issues(semantic_result.issues, show_non_blocking=show_all, verbose=verbose_issues)
        return

    dw_results = ModelValidationResults()
    if not skip_dw:
        dw_validator = DataWarehouseModelValidator(sql_client=cfg.sql_client, system_schema=cfg.mf_system_schema)
        dw_results = _data_warehouse_validations_runner(dw_validator=dw_validator, model=user_model, timeout=dw_timeout)

    merged_results = ModelValidationResults.merge(
        [semantic_result.issues, dw_results]
    )
    _print_issues(merged_results, show_non_blocking=show_all, verbose=verbose_issues)


if __name__ == "__main__":
    cli()
