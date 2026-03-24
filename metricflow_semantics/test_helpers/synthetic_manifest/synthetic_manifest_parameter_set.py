from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SyntheticManifestParameterSet:
    """Describes how to generate a synthetic manifest for performance testing.

    Goals are:
    * Allow modeling of similar patterns seen in production manifests.
    * Make generation straightforward.
    * Minimize the number of parameters required.

    Notes:
    * The synthetic manifest groups semantic models into two types - ones associated with simple metrics,
      and others containing dimensions.
    * A dimension with the same name does not appear in multiple semantic models.
    * Al semantic models contain a common entity so that any simple metric can be queried by any dimension.
    * The metric `depth` describes the number of hops that are required to get to the simple metric when following the
      definition tree.
    * Metrics at `depth=0` are simple metrics. Metrics at other depth values are derived.
    * Each metric is defined using all possible metrics at a lower depth.
    * The number of metrics that are generated with a given `depth` is called the `width`.
    * A random seed can be added later.
    """

    # The number of semantic models to generate that are associated with simple metrics.
    simple_metric_semantic_model_count: int
    # For each semantic model for simple metrics, the count of associated simple metrics.
    simple_metrics_per_semantic_model: int

    # The count of semantic models to generate that contain dimensions.
    dimension_semantic_model_count: int
    # For each semantic model containing dimension, the count of dimensions that it should contain.
    categorical_dimensions_per_semantic_model: int

    # See class docstring.
    max_metric_depth: int
    max_metric_width: int

    # The number of saved queries to generate and the number of elements in each.
    saved_query_count: int
    metrics_per_saved_query: int
    categorical_dimensions_per_saved_query: int

    # The name of the entity that is common to all semantic models.
    common_entity_name: str = "common_entity"
