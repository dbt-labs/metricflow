import datetime
import logging

import dateutil.parser
from _pytest.fixtures import FixtureRequest
from typing import Callable, Tuple

from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.metric import Metric
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.protocols.sql_client import SqlClient

logger = logging.getLogger(__name__)


def as_datetime(date_string: str) -> datetime.datetime:
    """Helper to convert a string like '2020-01-01' into a datetime object."""
    return dateutil.parser.parse(date_string)


def should_skip_multi_threaded(
    request: FixtureRequest,
    sql_client: SqlClient,
) -> bool:
    """Returns whether to skip this test because multi-threading is not supported by the DB"""
    if not sql_client.sql_engine_attributes.multi_threading_supported:
        logger.warning(
            f"Multi-threading is not supported with {sql_client.__class__.__name__}, so should skip "
            f"{request.node.fspath}::{request.node.name}`"
        )
        return True

    return False


def find_data_source_with(model: UserConfiguredModel, function: Callable[[DataSource], bool]) -> Tuple[DataSource, int]:
    """Returns a data source from the model which matches the criteria defined by the passed in function'

    This is useful because the order of data sources in the list is non determinant, thus it's impossible to
    hard code which data source you want by index. Using data source names isn't great for consistency because
    data sources change and might no longer have the necessary parts to be useful for a given test. This
    allows us to guarantee that a data source will be returned which meets the requirements of what a test needs,
    unless none of the data sources will work.
    """
    for index, data_source in enumerate(model.data_sources):
        if function(data_source):
            return data_source, index

    raise Exception("Unable to find a data_source matching function criteria")


def find_metric_with(model: UserConfiguredModel, function: Callable[[Metric], bool]) -> Tuple[Metric, int]:
    """Returns a metric from the model which matches the criteria defined by the passed in function'

    This is useful because the order of metrics in the list is non-determinant, thus it's impossible to
    hard code which metric you want by index. Using metric names isn't great for consistency because
    metrics change and might no longer have the necessary parts to be useful for a given test. This
    allows us to guarantee that a metric will be returned which meets the requirements of what a test needs,
    unless none of the metrics will work.
    """
    for index, metric in enumerate(model.metrics):
        if function(metric):
            return metric, index

    raise Exception("Unable to find a metric matching function criteria")
