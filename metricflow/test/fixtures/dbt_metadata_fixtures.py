import pytest
from typing import Any, Dict, List, Tuple

from dbt_metadata_client.dbt_metadata_api_schema import MetricNode


@pytest.fixture
def dbt_cats_model() -> Dict[str, Any]:  # type: ignore[misc]
    """A simple dbt ModelNode in dict form"""
    return {
        "name": "cats",
        "description": "So many cats, all the cats",
        "schema": "animals",
        "database": "earth_data",
        "columns": [
            {
                "name": "BREED",
                "type": "TEXT",
            },
            {
                "name": "ORIGIN",
                "type": "TEXT",
            },
            {
                "name": "MAX_AGE",
                "type": "NUMBER",
            },
            {"name": "AVERAGE_AGE", "type": "NUMBER"},
            {
                "name": "CREATED_AT",
                "type": "TIMESTAMP_TZ",
            },
            {
                "name": "DOMESTICATED",
                "type": "BOOLEAN",
            },
        ],
    }


@pytest.fixture
def num_domesticated_cats_metric(dbt_cats_model: Dict[str, Any]) -> Dict[str, Any]:  # type: ignore[misc]
    """A dbt `count` type MetricNode with a simple filter"""
    return {
        "name": "num_domesticated_cats",
        "description": "The number of domesticated breeds",
        "calculation_method": "count",
        "dimensions": ["breed", "origin", "max_age"],
        "timestamp": "created_at",
        "expression": "breed",
        "filters": [{"field": "domesticated", "operator": "=", "value": "true"}],
        "model": dbt_cats_model,
    }


@pytest.fixture
def num_cats_with_max_age_close_to_average_age_metric(dbt_cats_model: Dict[str, Any]) -> Dict[str, Any]:  # type: ignore[misc]
    """A dbt `count` type MetricNode with a complex-ish filter"""
    return {
        "name": "num_cats_with_max_age_close_to_average_age",
        "description": "The number of cat breeds where the difference between the max_age and average_age is less than or equal to 2",
        "calculation_method": "count",
        "dimensions": ["breed", "origin"],
        "timestamp": "created_at",
        "expression": "breed",
        "filters": [{"field": "2", "operator": ">=", "value": "(max_age - average_age)"}],
        "model": dbt_cats_model,
    }


@pytest.fixture
def dbt_json_metrics(  # type: ignore[misc]
    dbt_cats_model: Dict[str, Any],
    num_domesticated_cats_metric: Dict[str, Any],
    num_cats_with_max_age_close_to_average_age_metric: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """A list of JSON-ish dbt metrics"""

    return [
        num_domesticated_cats_metric,
        num_cats_with_max_age_close_to_average_age_metric,
        {
            "name": "num_cat_origins",
            "description": "The number of distinct origins cats come from",
            "calculation_method": "count_distinct",
            "dimensions": ["breed", "max_age"],
            "timestamp": "created_at",
            "expression": "origin",
            "filters": None,
            "model": dbt_cats_model,
        },
        {
            "name": "sum_of_cats_max_age_not_useful",
            "description": None,
            "calculation_method": "sum",
            "dimensions": ["breed", "origin"],
            "timestamp": "created_at",
            "expression": "max_age",
            "filters": None,
            "model": dbt_cats_model,
        },
        {
            "name": "average_cat_breed_max_age",
            "description": "The average max age for all cat breeds",
            "calculation_method": "average",
            "dimensions": ["breed", "origin"],
            "timestamp": "created_at",
            "expression": "max_age",
            "filters": None,
            "model": dbt_cats_model,
        },
        {
            "name": "min_cat_breed_max_age",
            "description": "The minimum max age for all cat breeds",
            "calculation_method": "min",
            "dimensions": ["breed", "origin"],
            "timestamp": "created_at",
            "expression": "max_age",
            "filters": None,
            "model": dbt_cats_model,
        },
        {
            "name": "max_cat_breed_max_age",
            "description": "The maximum max age for all cat breeds",
            "calculation_method": "max",
            "dimensions": ["breed", "origin"],
            "timestamp": "created_at",
            "expression": "max_age",
            "filters": None,
            "model": dbt_cats_model,
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
