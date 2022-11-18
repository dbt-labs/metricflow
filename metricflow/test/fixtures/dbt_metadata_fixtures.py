import pytest
from typing import Any, Dict, List, Tuple

from dbt_metadata_client.dbt_metadata_api_schema import MetricNode


@pytest.fixture
def dbt_json_metrics() -> List[Dict[str, Any]]:  # type: ignore[misc]
    """A list of JSON-ish dbt metrics"""

    cats_model = {
        "name": "cats",
        "description": "So many cats, all the cats",
        "schema": "animals",
        "database": "earth_data",
    }

    return [
        {
            "name": "num_domesticated_cats",
            "description": "The number of domesticated breeds",
            "calculation_method": "count",
            "dimensions": ["breed", "origin", "max_age"],
            "timestamp": "created_at",
            "expression": "breed",
            "filters": [{"field": "domesticated", "operator": "=", "value": "true"}],
            "model": cats_model,
        },
        {
            "name": "num_cat_origins",
            "description": "The number of distinct origins cats come from",
            "calculation_method": "count_distinct",
            "dimensions": ["breed", "max_age"],
            "timestamp": "created_at",
            "expression": "origin",
            "filters": None,
            "model": cats_model,
        },
        {
            "name": "sum_of_cats_max_age_not_useful",
            "description": None,
            "calculation_method": "sum",
            "dimensions": ["breed", "origin"],
            "timestamp": "created_at",
            "expression": "max_age",
            "filters": None,
            "model": cats_model,
        },
        {
            "name": "average_cat_breed_max_age",
            "description": "The average max age for all cat breeds",
            "calculation_method": "average",
            "dimensions": ["breed", "origin"],
            "timestamp": "created_at",
            "expression": "max_age",
            "filters": None,
            "model": cats_model,
        },
        {
            "name": "min_cat_breed_max_age",
            "description": "The minimum max age for all cat breeds",
            "calculation_method": "min",
            "dimensions": ["breed", "origin"],
            "timestamp": "created_at",
            "expression": "max_age",
            "filters": None,
            "model": cats_model,
        },
        {
            "name": "max_cat_breed_max_age",
            "description": "The maximum max age for all cat breeds",
            "calculation_method": "max",
            "dimensions": ["breed", "origin"],
            "timestamp": "created_at",
            "expression": "max_age",
            "filters": None,
            "model": cats_model,
        },
        {
            "name": "ratio_max_to_min_cat_breed_max_age",
            "description": "The ratio of the max max age to the min max age",
            "calculation_method": "derived",
            "dimensions": None,
            "timestamp": "created_at",
            "expression": "(max_cat_breed_max_age / min_cat_breed_max_age)",
            "dependsOn": [
                "metricflow_tests.metric.max_cat_breed_max_age",
                "metricflow_tests.metric.min_cat_breed_max_age",
            ],
            "filters": None,
            "model": None,
        },
    ]


@pytest.fixture
def dbt_metrics(dbt_json_metrics: List[Dict[str, Any]]) -> Tuple[MetricNode, ...]:  # type: ignore[misc]
    """A list of dbt MetricNodes"""
    metric_nodes: Tuple[MetricNode, ...] = tuple(MetricNode(json_data=metric_json) for metric_json in dbt_json_metrics)
    return metric_nodes
