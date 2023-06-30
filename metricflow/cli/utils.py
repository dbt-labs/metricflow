from __future__ import annotations

import datetime as dt
import logging
import pathlib
import traceback
from functools import update_wrapper, wraps
from typing import Any, Callable, List, Optional

import click
from dateutil.parser import parse

import metricflow.cli.custom_click_types as click_custom
from metricflow.cli.cli_context import CLIContext

logger = logging.getLogger(__name__)


# Click Options
def query_options(function: Callable) -> Callable:
    """Common options for a query."""
    function = click.option(
        "--order",
        type=click_custom.SequenceParamType(),
        help='Metrics or group bys to order by ("-" prefix for DESC). For example: --order -ds or --order ds,-revenue',
        required=False,
    )(function)
    function = click.option(
        "--limit",
        type=str,
        help="Limit the number of rows out using an int or leave blank for no limit. For example: --limit 100",
        callback=lambda ctx, param, value: validate_limit(value),
    )(function)
    function = click.option(
        "--where",
        type=str,
        default=None,
        help='SQL-like where statement provided as a string. For example: --where "revenue > 100"',
    )(function)
    function = start_end_time_options(function)
    function = click.option(
        "--group-by",
        type=click_custom.SequenceParamType(),
        default="",
        help="Dimensions and/or entities to group by: syntax is --group-by ds or for multiple group bys --group-by ds,org",
    )(function)
    function = click.option(
        "--metrics",
        type=click_custom.SequenceParamType(min_length=1),
        default="",
        help="Metrics to query for: syntax is --metrics bookings or for multiple metrics --metrics bookings,messages",
    )(function)
    return function


def start_end_time_options(function: Callable) -> Callable:
    """Options for start_time and end_time."""
    function = click.option(
        "--start-time",
        type=str,
        default=None,
        help="Optional iso8601 timestamp to constraint the start time of the data (inclusive)",
        callback=lambda ctx, param, value: convert_to_datetime(value),
    )(function)

    function = click.option(
        "--end-time",
        type=str,
        default=None,
        help="Optional iso8601 timestamp to constraint the end time of the data (inclusive)",
        callback=lambda ctx, param, value: convert_to_datetime(value),
    )(function)
    return function


# Parsers/Validators
def convert_to_datetime(datetime_str: Optional[str]) -> Optional[dt.datetime]:
    """Callback to convert string to datetime given as an iso8601 timestamp."""
    if datetime_str is None:
        return None

    try:
        return parse(datetime_str)
    except Exception:
        raise click.BadParameter("must be valid iso8601 timestamp")


def parse_comma_separated_inputs(value: Optional[str]) -> Optional[List[str]]:  # noqa: D
    # If comma exist, explode this into a list and return
    if value is None:
        return None
    if "," in value:
        return [i.strip() for i in value.split(",")]

    # Return a list of the single value
    return [value]


def validate_limit(limit: Optional[str]) -> Optional[int]:
    """Validates and transform limit input."""
    if limit and not limit.isnumeric():
        raise click.BadParameter("limit must be an int. For no limit, do not pass this argument")
    return int(limit) if limit else None


# Misc
def exception_handler(func: Callable[..., Any]) -> Callable[..., Any]:  # type: ignore[misc]
    """Decorator to handle exceptions."""

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:  # type: ignore[misc]
        try:
            func(*args, **kwargs)
        except Exception as e:
            # This will log to the file handlers registered in the root.
            logging.exception("Got an exception in the exception handler.")
            # Checks if CLIContext has verbose flag set

            if isinstance(args[0], CLIContext):
                cli_context: CLIContext = args[0]
                click.echo(f"\nERROR: {str(e)}\nLog file: {cli_context.log_file_path}")
            else:
                if not isinstance(args[0], CLIContext):
                    logger.error(
                        f"Missing {CLIContext.__name__} as the first argument to the function "
                        f"{getattr(func, '__name__', repr(func))}"
                    )
                click.echo(f"\nERROR: {str(e)}")
            if args and hasattr(args[0], "verbose") and args[0].verbose is True:
                click.echo(traceback.format_exc())

            exit(1)

    return wrapper


def dbt_project_file_exists() -> bool:
    """Check that the cwd is a dbt project root. Currently done by checking for existence of dbt_project.yml."""
    return pathlib.Path("dbt_project.yml").exists()


def error_if_not_in_dbt_project(func: Callable) -> Callable:
    """Decorator to output an error message and exit if caller is not in a root directory of a dbt project."""

    @click.pass_context
    def new_func(ctx: click.core.Context, *args: Any, **kwargs: Any) -> Any:  # type: ignore[misc]
        if not dbt_project_file_exists():
            click.echo(
                "❌ Unable to locate 'dbt_project.yml' in the current directory\n"
                "In order to run the MetricFlow CLI, you must be running in the root directory of a working dbt project.\n"
                "Please check out `https://docs.getdbt.com/reference/commands/init` if you want to get started on building a dbt project."
            )
            exit(1)
        return ctx.invoke(func, *args, **kwargs)

    return update_wrapper(new_func, func)
