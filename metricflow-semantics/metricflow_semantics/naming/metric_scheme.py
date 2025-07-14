from __future__ import annotations

from typing import Optional

from dbt_semantic_interfaces.references import MetricReference
from typing_extensions import override

from metricflow_semantics.errors.error_classes import InvalidQuerySyntax
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.naming.naming_scheme import QueryItemLocation, QueryItemNamingScheme
from metricflow_semantics.specs.instance_spec import InstanceSpec
from metricflow_semantics.specs.patterns.metric_pattern import MetricSpecPattern
from metricflow_semantics.specs.spec_set import group_spec_by_type


class MetricNamingScheme(QueryItemNamingScheme):
    """A naming scheme for metrics."""

    @override
    def input_str(self, instance_spec: InstanceSpec) -> Optional[str]:
        spec_set = group_spec_by_type(instance_spec)
        names = tuple(spec.element_name for spec in spec_set.metric_specs)

        if len(names) != 1:
            raise RuntimeError(f"Did not get 1 name for {instance_spec}. Got {names}")

        return names[0]

    @override
    def spec_pattern(
        self,
        input_str: str,
        semantic_manifest_lookup: SemanticManifestLookup,
        query_item_location: QueryItemLocation = QueryItemLocation.NON_ORDER_BY,
    ) -> MetricSpecPattern:
        input_str = input_str.lower()
        if not self.input_str_follows_scheme(
            input_str, semantic_manifest_lookup=semantic_manifest_lookup, query_item_location=query_item_location
        ):
            raise InvalidQuerySyntax(f"{repr(input_str)} does not follow this scheme.")
        return MetricSpecPattern(metric_reference=MetricReference(element_name=input_str))

    @override
    def input_str_follows_scheme(
        self,
        input_str: str,
        semantic_manifest_lookup: SemanticManifestLookup,
        query_item_location: QueryItemLocation = QueryItemLocation.NON_ORDER_BY,
    ) -> bool:
        return "(" not in input_str

    @override
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id()={hex(id(self))})"
