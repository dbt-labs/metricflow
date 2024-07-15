from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha1
from typing import Any, Sequence, Tuple

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from dbt_semantic_interfaces.type_enums import AggregationType

from metricflow_semantics.specs.entity_spec import LinklessEntitySpec
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.sql.sql_column_type import SqlColumnType


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

    @property
    def bucket_hash(self) -> str:
        """Returns the hash value used for grouping equivalent params."""
        values = [self.window_choice.name, self.name]
        values.extend(sorted(self.window_groupings))
        return hash_items(values)

    @property
    def linkable_specs(self) -> Sequence[LinkableInstanceSpec]:  # noqa: D102
        return (TimeDimensionSpec.from_name(self.name),) + tuple(
            LinklessEntitySpec.from_element_name(entity_name) for entity_name in self.window_groupings
        )

    def __eq__(self, other: Any) -> bool:  # type: ignore[misc]  # noqa: D105
        if not isinstance(other, NonAdditiveDimensionSpec):
            return False
        return self.bucket_hash == other.bucket_hash
