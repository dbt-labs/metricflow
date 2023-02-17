import copy
import datetime
import logging

import dateutil.parser
from _pytest.fixtures import FixtureRequest
from typing import Callable, Tuple, List

from metricflow.model.objects.entity import Entity
from metricflow.model.objects.metric import Metric
from dbt.dbt_semantic.objects.user_configured_model import UserConfiguredModel
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


def find_entity_with(model: UserConfiguredModel, function: Callable[[Entity], bool]) -> Tuple[Entity, int]:
    """Returns a entity from the model which matches the criteria defined by the passed in function'

    This is useful because the order of entities in the list is non determinant, thus it's impossible to
    hard code which entity you want by index. Using entity names isn't great for consistency because
    entities change and might no longer have the necessary parts to be useful for a given test. This
    allows us to guarantee that a entity will be returned which meets the requirements of what a test needs,
    unless none of the entities will work.
    """
    for index, entity in enumerate(model.entities):
        if function(entity):
            return entity, index

    raise Exception("Unable to find a entity matching function criteria")


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


