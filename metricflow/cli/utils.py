import datetime as dt
import logging
import os.path
import pathlib
import traceback
from functools import wraps
from typing import Any, Callable, List, Optional, Tuple

import click
from dateutil.parser import parse

from metricflow.cli.cli_context import CLIContext
from metricflow.configuration.config_builder import ConfigKey
from metricflow.configuration.constants import (
    CONFIG_DWH_CREDS_PATH,
    CONFIG_DWH_DB,
    CONFIG_DWH_DIALECT,
    CONFIG_DWH_HOST,
    CONFIG_DWH_PASSWORD,
    CONFIG_DWH_PORT,
    CONFIG_DWH_PROJECT_ID,
    CONFIG_DWH_SCHEMA,
    CONFIG_DWH_USER,
    CONFIG_DWH_WAREHOUSE,
    CONFIG_EMAIL,
    CONFIG_MODEL_PATH,
)
from metricflow.model.validations.validator_helpers import ValidationIssueLevel
from metricflow.sql_clients.common_client import SqlDialect

logger = logging.getLogger(__name__)

# MetricFlow config keys
MF_CONFIG_KEYS = (
    ConfigKey(key=CONFIG_EMAIL, comment="Optional"),
    ConfigKey(
        key=CONFIG_MODEL_PATH,
        value=f"{pathlib.Path.home()}/.metricflow/sample_models",
        comment="Path to directory containing defined models (Leave until after DWH setup)",
    ),
    ConfigKey(key=CONFIG_DWH_SCHEMA),
)
# BigQuery config keys
MF_BIGQUERY_KEYS = (
    ConfigKey(
        key=CONFIG_DWH_CREDS_PATH, comment="Provide the path to the BigQuery credential file, ignore to use ADC auth"
    ),
    ConfigKey(
        key=CONFIG_DWH_PROJECT_ID, comment="Provide the GCP Project ID, ignore if using service account credentials"
    ),
    ConfigKey(key=CONFIG_DWH_DIALECT, value=SqlDialect.BIGQUERY.value),
)

# Redshift config keys
MF_POSTGRESQL_KEYS = (
    ConfigKey(key=CONFIG_DWH_DB),
    ConfigKey(key=CONFIG_DWH_PASSWORD, comment="Password associated with the provided user"),
    ConfigKey(key=CONFIG_DWH_USER, comment="Username for the data warehouse"),
    ConfigKey(key=CONFIG_DWH_PORT),
    ConfigKey(key=CONFIG_DWH_HOST, comment="Host name"),
    ConfigKey(key=CONFIG_DWH_DIALECT, value=SqlDialect.POSTGRESQL.value),
)

# Redshift config keys
MF_REDSHIFT_KEYS = (
    ConfigKey(key=CONFIG_DWH_DB),
    ConfigKey(key=CONFIG_DWH_PASSWORD, comment="Password associated with the provided user"),
    ConfigKey(key=CONFIG_DWH_USER, comment="Username for the data warehouse"),
    ConfigKey(key=CONFIG_DWH_PORT),
    ConfigKey(key=CONFIG_DWH_HOST, comment="Host name"),
    ConfigKey(key=CONFIG_DWH_DIALECT, value=SqlDialect.REDSHIFT.value),
)
# Snowflake config keys
MF_SNOWFLAKE_KEYS = (
    ConfigKey(key=CONFIG_DWH_WAREHOUSE, comment="Provide the warehouse to use"),
    ConfigKey(key=CONFIG_DWH_DB),
    ConfigKey(key=CONFIG_DWH_PASSWORD, comment="Password associated with the provided user"),
    ConfigKey(key=CONFIG_DWH_USER, comment="Username for the data warehouse"),
    ConfigKey(key=CONFIG_DWH_HOST, comment="Snowflake account name"),
    ConfigKey(key=CONFIG_DWH_DIALECT, value=SqlDialect.SNOWFLAKE.value),
)


def generate_duckdb_demo_keys(config_dir: str) -> Tuple[ConfigKey, ...]:
    """Generate configuration keys for DuckDB with a file in the config_dir."""
    return (
        ConfigKey(
            key=CONFIG_DWH_DB, value=os.path.join(config_dir, "duck.db"), comment="For DuckDB, this is the data file."
        ),
        ConfigKey(key=CONFIG_DWH_DIALECT, value="duckdb"),
        ConfigKey(key=CONFIG_DWH_SCHEMA, value="mf_demo"),
    )


# Click Options
def query_options(function: Callable) -> Callable:
    """Common options for a query"""
    function = separated_by_comma_option(
        "--order",
        help_msg='Metrics or dimensions to order by ("-" prefix for DESC). For example: --order -ds or --order ds,-revenue',
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
    function = separated_by_comma_option(
        "--dimensions",
        help_msg="Dimensions to group by: syntax is --dimensions ds or for multiple dimensions --dimensions ds,org",
    )(function)
    function = separated_by_comma_option(
        "--metrics",
        help_msg="Metrics to query for: syntax is --metrics bookings or for multiple metrics --metrics bookings,messages",
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


def separated_by_comma_option(option_name: str, help_msg: str = "", required: bool = True) -> Callable:
    """Parse input containing a string separated by commma to a List."""

    def wraps(function: Callable) -> Callable:
        function = click.option(
            option_name,
            required=required,
            help=help_msg,
            callback=lambda ctx, param, value: parse_comma_separated_inputs(value),
        )(function)
        return function

    return wraps


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
    def wrapper(*args, **kwargs):  # type: ignore
        try:
            func(*args, **kwargs)
        except Exception as e:
            # This will log to the file handlers registered in the root.
            logging.exception("Got an exception in the exception handler.")
            # Checks if CLIContext has verbose flag set

            if isinstance(args[0], CLIContext):
                cli_context: CLIContext = args[0]
                click.echo(f"\nERROR: {str(e)}\nLog file: {cli_context.config.log_file_path}")
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


def build_validation_header_msg(level: ValidationIssueLevel) -> str:
    """Builds the header message with colour."""
    colour_map = {
        ValidationIssueLevel.WARNING: "cyan",
        ValidationIssueLevel.ERROR: "bright_red",
        ValidationIssueLevel.FATAL: "bright_red",
        ValidationIssueLevel.FUTURE_ERROR: "bright_yellow",
    }

    return click.style(level.name, bold=True, fg=colour_map[level])
