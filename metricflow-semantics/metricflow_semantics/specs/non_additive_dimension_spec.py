from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, Sequence, Tuple

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from dbt_semantic_interfaces.type_enums import AggregationType, TimeGranularity

from metricflow_semantics.model.semantics.simple_metric_input import SimpleMetricInput
from metricflow_semantics.naming.linkable_spec_name import DUNDER
from metricflow_semantics.specs.entity_spec import LinklessEntitySpec
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.time.granularity import ExpandedTimeGranularity
from metricflow_semantics.toolkit.id_helpers import mf_sha1_iterables
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

if TYPE_CHECKING:
    from metricflow.dataflow.builder.simple_metric_input_spec_properties import SimpleMetricInputSpecProperties

logger = logging.getLogger(__file__)


@dataclass(frozen=True)
class NonAdditiveDimensionSpec(SerializableDataclass):
    """Spec representing non-additive dimension parameters for use within a SimpleMetricInputSpec.

    This is sourced from the NonAdditiveDimensionParameters model object, which provides the parsed parameter set,
    while the spec contains the information needed for dataflow plan operations
    """

    name: str
    window_choice: AggregationType
    window_groupings: Tuple[str, ...] = ()

    @staticmethod
    def create_from_simple_metric_input(  # noqa: D102
        simple_metric_input: SimpleMetricInput,
    ) -> Optional[NonAdditiveDimensionSpec]:
        if simple_metric_input.non_additive_dimension is None:
            return None

        return NonAdditiveDimensionSpec(
            name=simple_metric_input.non_additive_dimension.name,
            window_choice=simple_metric_input.non_additive_dimension.window_choice,
            window_groupings=tuple(sorted(simple_metric_input.non_additive_dimension.window_groupings)),
        )

    def __post_init__(self) -> None:
        """Post init validator to ensure names with double-underscores are not allowed."""
        # TODO: [custom granularity] change this to an assertion once we're sure there aren't exceptions
        if not self.name.find(DUNDER) == -1:
            logger.warning(
                LazyFormat(
                    lambda: f"Non-additive dimension spec references a dimension name `{self.name}`, with added annotations, but it "
                    "should be a simple element name reference. This should have been blocked by model validation!"
                )
            )

    @property
    def bucket_hash(self) -> str:
        """Returns the hash value used for grouping equivalent params."""
        return mf_sha1_iterables([self.window_choice.name, self.name], sorted(self.window_groupings))

    def linkable_specs(self, non_additive_dimension_grain: TimeGranularity) -> Sequence[LinkableInstanceSpec]:
        """Return the set of linkable specs referenced by the NonAdditiveDimensionSpec.

        In practice, the name will always point to a time dimension. This method requires the time granularity
        provided in the model Dimension definition, which is why the input is typed as an enum value rather than
        an expanded granularity object - custom granularities are not eligible for consideration here.
        """
        return (
            TimeDimensionSpec(
                element_name=self.name,
                entity_links=(),
                time_granularity=ExpandedTimeGranularity.from_time_granularity(non_additive_dimension_grain),
            ),
        ) + tuple(LinklessEntitySpec.from_element_name(entity_name) for entity_name in self.window_groupings)

    @property
    def window_groupings_as_specs(self) -> Tuple[LinklessEntitySpec, ...]:  # noqa: D102
        return tuple(LinklessEntitySpec.from_element_name(entity_name) for entity_name in self.window_groupings)

    def name_as_time_dimension_spec(  # noqa: D102
        self, spec_properties: SimpleMetricInputSpecProperties
    ) -> TimeDimensionSpec:
        return TimeDimensionSpec(
            element_name=self.name,
            entity_links=(),
            time_granularity=ExpandedTimeGranularity.from_time_granularity(spec_properties.agg_time_dimension_grain),
        )
