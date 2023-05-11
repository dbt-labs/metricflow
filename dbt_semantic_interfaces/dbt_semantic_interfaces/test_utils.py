import datetime
import logging

import dateutil.parser
from typing import Callable, Tuple

from dbt_semantic_interfaces.objects.semantic_model import SemanticModel
from dbt_semantic_interfaces.objects.metric import Metric
from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest

logger = logging.getLogger(__name__)


def as_datetime(date_string: str) -> datetime.datetime:
    """Helper to convert a string like '2020-01-01' into a datetime object."""
    return dateutil.parser.parse(date_string)


def find_semantic_model_with(
    model: SemanticManifest, function: Callable[[SemanticModel], bool]
) -> Tuple[SemanticModel, int]:
    """Returns a semantic model from the model which matches the criteria defined by the passed in function'

    This is useful because the order of semantic models in the list is non determinant, thus it's impossible to
    hard code which semantic model you want by index. Using semantic model names isn't great for consistency because
    semantic models change and might no longer have the necessary parts to be useful for a given test. This
    allows us to guarantee that a semantic model will be returned which meets the requirements of what a test needs,
    unless none of the semantic models will work.
    """
    for index, semantic_model in enumerate(model.semantic_models):
        if function(semantic_model):
            return semantic_model, index

    raise Exception("Unable to find a semantic_model matching function criteria")


def find_metric_with(model: SemanticManifest, function: Callable[[Metric], bool]) -> Tuple[Metric, int]:
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
