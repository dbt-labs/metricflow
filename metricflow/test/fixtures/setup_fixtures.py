import datetime
import logging
from dataclasses import dataclass

import _pytest.config
import pytest
from _pytest.fixtures import FixtureRequest

from metricflow.configuration.env_var import EnvironmentVariable
from metricflow.object_utils import random_id

logger = logging.getLogger(__name__)


@dataclass
class MetricFlowTestSessionState:
    """State that is shared between tests during a testing session."""

    sql_engine_url: str
    sql_engine_password: str
    # Where MF system tables should be stored.
    mf_system_schema: str
    # Where tables for test data sets should be stored.
    mf_source_schema: str

    # Number of plans that were displayed to the user.
    display_plans: bool
    # Whether to overwrite any text files that were generated.
    overwrite_snapshots: bool
    # Number of plans that were displayed to the user.
    plans_displayed: int
    # Maximum number of plans to display to the user. If this is exceeded, an exception should be thrown. This is to
    # help avoid a case where an excessive number of plans are displayed. This could happen if the user accidentally
    # runs all tests when they were just looking to run one test and visualize the associated plan.
    max_plans_displayed: int


DISPLAY_PLANS_CLI_FLAG = "--display-plans"
OVERWRITE_SNAPSHOTS_CLI_FLAG = "--overwrite-snapshots"


def pytest_addoption(parser: _pytest.config.argparsing.Parser) -> None:
    """Add options for running pytest through the CLI."""
    parser.addoption(DISPLAY_PLANS_CLI_FLAG, action="store_true", help="Displays plans as SVGs in a browser tab if set")
    parser.addoption(
        OVERWRITE_SNAPSHOTS_CLI_FLAG,
        action="store_true",
        help="Overwrites existing snapshots by ones generated during this testing session",
    )


class MetricFlowTestEnvironmentVariables:
    """Environment variables to setup the testing environment."""

    MF_SQL_ENGINE_URL = EnvironmentVariable("MF_SQL_ENGINE_URL")
    MF_SQL_ENGINE_PASSWORD = EnvironmentVariable("MF_SQL_ENGINE_PASSWORD")


@pytest.fixture(scope="session")
def mf_test_session_state(request: FixtureRequest) -> MetricFlowTestSessionState:  # noqa: D
    engine_url = MetricFlowTestEnvironmentVariables.MF_SQL_ENGINE_URL.get_optional()
    if engine_url is None:
        logger.info(f"{MetricFlowTestEnvironmentVariables.MF_SQL_ENGINE_URL.name} has not been set, so using SQLite")
        engine_url = "sqlite://"
    engine_password = MetricFlowTestEnvironmentVariables.MF_SQL_ENGINE_PASSWORD.get_optional() or ""

    current_time = datetime.datetime.now().strftime("%Y_%m_%d")
    random_suffix = random_id()
    mf_system_schema = f"mf_test_{current_time}_{random_suffix}"
    mf_source_schema = mf_system_schema

    return MetricFlowTestSessionState(
        sql_engine_url=engine_url,
        sql_engine_password=engine_password,
        mf_system_schema=mf_system_schema,
        mf_source_schema=mf_source_schema,
        display_plans=bool(request.config.getoption(DISPLAY_PLANS_CLI_FLAG, default=False)),
        overwrite_snapshots=bool(request.config.getoption(OVERWRITE_SNAPSHOTS_CLI_FLAG, default=False)),
        plans_displayed=0,
        max_plans_displayed=6,
    )
