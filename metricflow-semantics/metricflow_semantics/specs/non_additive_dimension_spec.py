from __future__ import annotations

import logging
from dataclasses import dataclass
from hashlib import sha1
from typing import Any, Sequence, Tuple

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from dbt_semantic_interfaces.type_enums import AggregationType, TimeGranularity

from metricflow_semantics.naming.linkable_spec_name import DUNDER
from metricflow_semantics.specs.entity_spec import LinklessEntitySpec
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.sql.sql_column_type import SqlColumnType

logger = logging.getLogger(__file__)


def hash_items(items: Sequence[SqlColumnType]) -> str:
    """Produces a hash from a list of strings."""
    hash_builder = sha1()
    for item in items:
        hash_builder.update(str(item).encode("utf-8"))
    return hash_builder.hexdigest()


@dataclass(frozen=True)
class NonAdditiveDimensionSpec(SerializableDataclass):
    """Spec representing non-additive dimension parameters for use within a MeasureSpec.

    This is sourced from the NonAdditiveDimensionParameters model object, which provides the parsed parameter set,
    while the spec contains the information needed for dataflow plan operations
    """

    name: str
    window_choice: AggregationType
    window_groupings: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Post init validator to ensure names with double-underscores are not allowed."""
        # TODO: [custom granularity] change this to an assertion once we're sure there aren't exceptions
        if not self.name.find(DUNDER) == -1:
            logger.warning(
                f"Non-additive dimension spec references a dimension name `{self.name}`, with added annotations, but it "
                "should be a simple element name reference. This should have been blocked by model validation!"
            )

    @property
    def bucket_hash(self) -> str:
        """Returns the hash value used for grouping equivalent params."""
        values = [self.window_choice.name, self.name]
        values.extend(sorted(self.window_groupings))
        return hash_items(values)

    def linkable_specs(  # noqa: D102
        self, non_additive_dimension_grain: TimeGranularity
    ) -> Sequence[LinkableInstanceSpec]:
        return (
            TimeDimensionSpec(element_name=self.name, entity_links=(), time_granularity=non_additive_dimension_grain),
        ) + tuple(LinklessEntitySpec.from_element_name(entity_name) for entity_name in self.window_groupings)

    def __eq__(self, other: Any) -> bool:  # type: ignore[misc]  # noqa: D105
        if not isinstance(other, NonAdditiveDimensionSpec):
            return False
        return self.bucket_hash == other.bucket_hash
