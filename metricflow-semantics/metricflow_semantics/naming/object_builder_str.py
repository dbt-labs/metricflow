from __future__ import annotations

import logging
from typing import Optional, Sequence

from dbt_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    MetricCallParameterSet,
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.naming.keywords import DUNDER
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from typing_extensions import override

from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.specs.instance_spec import InstanceSpec
from metricflow_semantics.specs.spec_set import InstanceSpecSet, InstanceSpecSetTransform, group_spec_by_type

logger = logging.getLogger(__name__)


class ObjectBuilderNameConverter:
    """Methods for converting classes related to the object-builder naming scheme to strings.

    String conversions are necessary for parsing user input and generating error messages.

    TODO: CallParameterSets / QueryParameter objects need restructuring.
    """

    @staticmethod
    def input_str_from_entity_call_parameter_set(parameter_set: EntityCallParameterSet) -> str:  # noqa: D102
        initializer_parameter_str = ObjectBuilderNameConverter.initializer_parameter_str(
            element_name=parameter_set.entity_reference.element_name,
            entity_links=parameter_set.entity_path,
            group_by=(),
            time_granularity_name=None,
            date_part=None,
        )
        return f"Entity({initializer_parameter_str})"

    @staticmethod
    def input_str_from_metric_call_parameter_set(parameter_set: MetricCallParameterSet) -> str:  # noqa: D102
        initializer_parameter_str = ObjectBuilderNameConverter.initializer_parameter_str(
            element_name=parameter_set.metric_reference.element_name,
            entity_links=(),
            group_by=tuple(
                EntityReference(element_name=group_by_ref.element_name) for group_by_ref in parameter_set.group_by
            ),
            time_granularity_name=None,
            date_part=None,
        )
        return f"Metric({initializer_parameter_str})"

    @staticmethod
    def initializer_parameter_str(
        element_name: str,
        entity_links: Sequence[EntityReference],
        group_by: Sequence[EntityReference],
        time_granularity_name: Optional[str],
        date_part: Optional[DatePart],
    ) -> str:
        """Return the parameters that should go in the initializer.

        e.g. `'user__country', time_granularity_name='month'`
        """
        initializer_parameters = []
        entity_link_names = list(entity_link.element_name for entity_link in entity_links)
        if len(entity_link_names) > 0:
            initializer_parameters.append(repr(entity_link_names[-1] + DUNDER + element_name))
        else:
            initializer_parameters.append(repr(element_name))
        if time_granularity_name is not None:
            initializer_parameters.append(
                f"'{time_granularity_name}'",
            )
        if date_part is not None:
            initializer_parameters.append(f"date_part_name={repr(date_part.value)}")
        if len(entity_link_names) > 1:
            initializer_parameters.append(f"entity_path={repr(entity_link_names[:-1])}")
        if group_by:
            initializer_parameters.append(f"group_by={[group_by_ref.element_name for group_by_ref in group_by]}")

        return ", ".join(initializer_parameters)

    class _ObjectBuilderNameTransform(InstanceSpecSetTransform[Sequence[str]]):
        """Transforms specs into strings following the object builder scheme."""

        @override
        def transform(self, spec_set: InstanceSpecSet) -> Sequence[str]:
            names_to_return = []

            for entity_spec in spec_set.entity_specs:
                initializer_parameter_str = ObjectBuilderNameConverter.initializer_parameter_str(
                    element_name=entity_spec.element_name,
                    entity_links=entity_spec.entity_links,
                    group_by=(),
                    time_granularity_name=None,
                    date_part=None,
                )
                names_to_return.append(f"Entity({initializer_parameter_str})")

            for dimension_spec in spec_set.dimension_specs:
                initializer_parameter_str = ObjectBuilderNameConverter.initializer_parameter_str(
                    element_name=dimension_spec.element_name,
                    entity_links=dimension_spec.entity_links,
                    group_by=(),
                    time_granularity_name=None,
                    date_part=None,
                )
                names_to_return.append(f"Dimension({initializer_parameter_str})")

            for time_dimension_spec in spec_set.time_dimension_specs:
                initializer_parameter_str = ObjectBuilderNameConverter.initializer_parameter_str(
                    element_name=time_dimension_spec.element_name,
                    entity_links=time_dimension_spec.entity_links,
                    group_by=(),
                    time_granularity_name=time_dimension_spec.time_granularity.name,
                    date_part=time_dimension_spec.date_part,
                )
                names_to_return.append(f"TimeDimension({initializer_parameter_str})")

            for group_by_metric_spec in spec_set.group_by_metric_specs:
                initializer_parameter_str = ObjectBuilderNameConverter.initializer_parameter_str(
                    element_name=group_by_metric_spec.element_name,
                    entity_links=(),
                    group_by=group_by_metric_spec.entity_links,
                    time_granularity_name=None,
                    date_part=None,
                )
                names_to_return.append(f"Metric({initializer_parameter_str})")

            return names_to_return

    @staticmethod
    def input_str_from_spec(instance_spec: InstanceSpec) -> Optional[str]:  # noqa: D102
        names = ObjectBuilderNameConverter._ObjectBuilderNameTransform().transform(group_spec_by_type(instance_spec))

        if len(names) == 0:
            return None
        elif len(names) > 1:
            raise RuntimeError(str(LazyFormat("Expected at most one name", instance_spec=instance_spec, names=names)))

        return names[0]

    @staticmethod
    def input_str_from_dimension_call_parameter_set(parameter_set: DimensionCallParameterSet) -> str:  # noqa: D102
        initializer_parameter_str = ObjectBuilderNameConverter.initializer_parameter_str(
            element_name=parameter_set.dimension_reference.element_name,
            entity_links=parameter_set.entity_path,
            group_by=(),
            time_granularity_name=None,
            date_part=None,
        )
        return f"Dimension({initializer_parameter_str})"

    @staticmethod
    def input_str_from_time_dimension_call_parameter_set(  # noqa: D102
        parameter_set: TimeDimensionCallParameterSet,
    ) -> str:
        initializer_parameter_str = ObjectBuilderNameConverter.initializer_parameter_str(
            element_name=parameter_set.time_dimension_reference.element_name,
            entity_links=parameter_set.entity_path,
            group_by=(),
            time_granularity_name=None,
            date_part=None,
        )
        return f"TimeDimension({initializer_parameter_str})"
