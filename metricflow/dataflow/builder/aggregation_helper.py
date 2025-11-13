from __future__ import annotations

import logging
from collections.abc import Mapping
from functools import cached_property
from typing import Optional, Tuple

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from metricflow_semantics.specs.simple_metric_input_spec import SimpleMetricInputSpec, SimpleMetricRecipe
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.merger import Mergeable
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from typing_extensions import override

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class InstanceAliasMapping(Mergeable, SerializableDataclass):
    """Stores the mapping for how instances should be aliased (i.e. given a different column name)."""

    _element_name_and_alias_items: Tuple[Tuple[str, str], ...]

    @staticmethod
    def create(element_name_to_alias: Optional[Mapping[str, str]] = None) -> InstanceAliasMapping:  # noqa: D102
        if element_name_to_alias is None:
            return InstanceAliasMapping(_element_name_and_alias_items=())

        return InstanceAliasMapping(
            _element_name_and_alias_items=tuple(
                (element_name, alias) for element_name, alias in element_name_to_alias.items()
            )
        )

    @cached_property
    def element_name_to_alias(self) -> Mapping[str, str]:  # noqa: D102
        return {element_name: alias for element_name, alias in self._element_name_and_alias_items}

    def aliased_spec(self, spec: SimpleMetricInputSpec) -> Optional[SimpleMetricInputSpec]:
        """If this mapping contains an alias for the given spec, return the spec with the alias."""
        alias = self.element_name_to_alias.get(spec.element_name)
        if alias is None:
            return None
        return SimpleMetricInputSpec(
            element_name=alias,
            fill_nulls_with=spec.fill_nulls_with,
        )

    @override
    def merge(self, other: InstanceAliasMapping) -> InstanceAliasMapping:
        if self.has_conflict(other):
            raise RuntimeError(
                LazyFormat(
                    "Can't merge alias mappings with conflicting items",
                    self=self,
                    other=other,
                )
            )
        return InstanceAliasMapping.create({**self.element_name_to_alias, **other.element_name_to_alias})

    @classmethod
    @override
    def empty_instance(cls) -> InstanceAliasMapping:
        return InstanceAliasMapping(_element_name_and_alias_items=())

    def has_conflict(self, other: InstanceAliasMapping) -> bool:  # noqa: D102
        other_element_name_to_alias = other.element_name_to_alias
        other_aliases = set(other.element_name_to_alias.values())
        for element_name, alias in self.element_name_to_alias.items():
            other_alias = other_element_name_to_alias.get(element_name)
            if other_alias is not None:
                if alias != other_alias:
                    return True
            elif alias in other_aliases:
                return True
        return False


@fast_frozen_dataclass()
class NullFillValueMapping(Mergeable, SerializableDataclass):
    """Stores the mapping for which instances should have null values set to a configured value."""

    _element_name_and_null_fill_value_items: Tuple[Tuple[str, int], ...]

    @staticmethod
    def create(  # noqa: D102
        element_name_to_null_fill_value: Optional[Mapping[str, int]] = None
    ) -> NullFillValueMapping:  # noqa: D102
        if element_name_to_null_fill_value is None:
            return NullFillValueMapping(_element_name_and_null_fill_value_items=())
        return NullFillValueMapping(
            _element_name_and_null_fill_value_items=tuple(
                (element_name, null_fill_value)
                for element_name, null_fill_value in element_name_to_null_fill_value.items()
            )
        )

    @staticmethod
    def create_from_simple_metric_recipe(  # noqa: D102
        simple_metric_recipe: SimpleMetricRecipe,
    ) -> NullFillValueMapping:
        null_fill_value = simple_metric_recipe.simple_metric_input.fill_nulls_with
        if null_fill_value is None:
            return NullFillValueMapping(_element_name_and_null_fill_value_items=())

        return NullFillValueMapping(
            _element_name_and_null_fill_value_items=((simple_metric_recipe.simple_metric_input.name, null_fill_value),)
        )

    @cached_property
    def element_name_to_null_fill_value(self) -> Mapping[str, int]:  # noqa: D102
        result = {}
        for element_name, alias in self._element_name_and_null_fill_value_items:
            result[element_name] = alias
        return result

    def null_fill_value_spec(self, spec: SimpleMetricInputSpec) -> Optional[SimpleMetricInputSpec]:
        """If this mapping contains a null fill value for the given spec, return the spec with the value set."""
        null_fill_value = self.element_name_to_null_fill_value.get(spec.element_name)
        if null_fill_value is None:
            return None
        return SimpleMetricInputSpec(
            element_name=spec.element_name,
            fill_nulls_with=null_fill_value,
        )

    @override
    def merge(self, other: NullFillValueMapping) -> NullFillValueMapping:
        element_name_to_null_fill_value: dict[str, int] = {**self.element_name_to_null_fill_value}
        for element_name, other_null_fill_value in other._element_name_and_null_fill_value_items:
            current_null_fill_value = element_name_to_null_fill_value.get(element_name)
            if current_null_fill_value is not None and current_null_fill_value != other_null_fill_value:
                raise RuntimeError(
                    LazyFormat(
                        "Can't merge fill value mappings with conflicting items.",
                        element_name=element_name,
                        current_null_fill_value=current_null_fill_value,
                        other_null_fill_value=other_null_fill_value,
                    )
                )
            element_name_to_null_fill_value[element_name] = other_null_fill_value
        return NullFillValueMapping.create(element_name_to_null_fill_value)

    @classmethod
    @override
    def empty_instance(cls) -> NullFillValueMapping:
        return NullFillValueMapping(_element_name_and_null_fill_value_items=())
