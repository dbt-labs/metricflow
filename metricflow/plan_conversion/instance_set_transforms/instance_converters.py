"""Various classes that perform a transformation on instance sets."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.references import EntityReference, MetricReference, SemanticModelReference
from dbt_semantic_interfaces.type_enums import MetricType
from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.aggregation_properties import AggregationState
from metricflow_semantics.instances import (
    DimensionInstance,
    EntityInstance,
    GroupByMetricInstance,
    InstanceSet,
    InstanceSetTransform,
    MdoInstance,
    MetadataInstance,
    MetricInstance,
    SimpleMetricInputInstance,
    TimeDimensionInstance,
)
from metricflow_semantics.model.semantics.metric_lookup import MetricLookup
from metricflow_semantics.model.semantics.semantic_model_lookup import SemanticModelLookup
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.instance_spec import InstanceSpec, LinkableInstanceSpec
from metricflow_semantics.specs.spec_set import InstanceSpecSet
from metricflow_semantics.sql.sql_exprs import (
    SqlAggregateFunctionExpression,
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlExpressionNode,
    SqlFunction,
    SqlFunctionExpression,
    SqlStringExpression,
)
from metricflow_semantics.toolkit.assert_one_arg import assert_exactly_one_arg_set
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from more_itertools import bucket
from typing_extensions import TypeVar

from metricflow.dataflow.builder.aggregation_helper import InstanceAliasMapping, NullFillValueMapping
from metricflow.dataflow.nodes.join_to_base import ValidityWindowJoinDescription
from metricflow.plan_conversion.select_column_gen import SelectColumnSet
from metricflow.sql.sql_plan import (
    SqlSelectColumn,
)

logger = logging.getLogger(__name__)

InstanceT = TypeVar("InstanceT", bound=MdoInstance)


@dataclass(frozen=True)
class _DimensionValidityParams:
    """Helper dataclass for managing dimension validity properties."""

    dimension_name: str
    time_granularity: TimeGranularity
    date_part: Optional[DatePart] = None


class CreateValidityWindowJoinDescription(InstanceSetTransform[Optional[ValidityWindowJoinDescription]]):
    """Create and return a ValidityWindowJoinDescription based on the given InstanceSet.

    During join resolution we need to determine whether or not a given data set represents a
    Type II SCD dataset - i.e., one with a validity window defined on each row. This requires
    checking the set of dimension instances and determining whether or not those originate from
    an SCD source, and extracting validity window information accordingly.
    """

    def __init__(self, semantic_model_lookup: SemanticModelLookup) -> None:
        """Initializer. The SemanticModelLookup is needed for getting the original model definition."""
        self._semantic_model_lookup = semantic_model_lookup

    def _get_validity_window_dimensions_for_semantic_model(
        self, semantic_model_reference: SemanticModelReference
    ) -> Optional[Tuple[_DimensionValidityParams, _DimensionValidityParams]]:
        """Returns a 2-tuple (start, end) of validity window dimensions info, if any exist in the semantic model."""
        semantic_model = self._semantic_model_lookup.get_by_reference(semantic_model_reference)
        assert semantic_model, f"Could not find semantic model {semantic_model_reference} after data set conversion!"

        start_dim = semantic_model.validity_start_dimension
        end_dim = semantic_model.validity_end_dimension

        # We do this instead of relying on has_validity_dimensions because this also does type refinement
        if not start_dim or not end_dim:
            return None

        assert start_dim.type_params, "Typechecker hint - validity info cannot exist without type params"
        assert end_dim.type_params, "Typechecker hint - validity info cannot exist without type params"

        return (
            _DimensionValidityParams(
                dimension_name=start_dim.name, time_granularity=start_dim.type_params.time_granularity
            ),
            _DimensionValidityParams(
                dimension_name=end_dim.name, time_granularity=end_dim.type_params.time_granularity
            ),
        )

    def transform(self, instance_set: InstanceSet) -> Optional[ValidityWindowJoinDescription]:
        """Find the Time Dimension specs defining a validity window, if any, and return it.

        This currently throws an exception if more than one such window is found, and effectively prevents
        us from processing a dataset composed of a join between two SCD semantic models. This restriction is in
        place as a temporary simplification - if there is need for this feature we can enable it.
        """
        semantic_model_to_window: Dict[SemanticModelReference, ValidityWindowJoinDescription] = {}
        instances_by_semantic_model = bucket(
            instance_set.time_dimension_instances, lambda x: x.origin_semantic_model_reference.semantic_model_reference
        )
        for semantic_model_reference in instances_by_semantic_model:
            validity_dims = self._get_validity_window_dimensions_for_semantic_model(semantic_model_reference)
            if validity_dims is None:
                continue

            start_dim, end_dim = validity_dims
            specs = {instance.spec for instance in instances_by_semantic_model[semantic_model_reference]}
            start_specs = [
                spec
                for spec in specs
                if spec.element_name == start_dim.dimension_name
                # TODO: [custom_granularity] - support custom granularities for SCDs. Note this requires
                # addition of SCD support for window sub-selection, similar to what we do for cumulative metrics
                # when we group by a different time grain (e.g., select last_value from window, etc.)
                and not spec.has_custom_grain
                and spec.base_granularity == start_dim.time_granularity
                and spec.date_part == start_dim.date_part
            ]
            end_specs = [
                spec
                for spec in specs
                if spec.element_name == end_dim.dimension_name
                # TODO: [custom_granularity] - support custom granularities for SCDs. Note this requires
                # addition of SCD support for window sub-selection, similar to what we do for cumulative metrics
                # when we group by a different time grain (e.g., select last_value from window, etc.)
                and not spec.has_custom_grain
                and spec.base_granularity == end_dim.time_granularity
                and spec.date_part == end_dim.date_part
            ]
            linkless_start_specs = {spec.without_entity_links for spec in start_specs}
            linkless_end_specs = {spec.without_entity_links for spec in end_specs}
            assert len(linkless_start_specs) == 1 and len(linkless_end_specs) == 1, (
                f"Did not find exactly one pair of specs from semantic model `{semantic_model_reference}` matching the validity "
                f"window end points defined in the semantic model. This means we cannot process an SCD join, because we "
                f"require exactly one validity window to be specified for the query! The window in the semantic model "
                f"is defined by start dimension `{start_dim}` and end dimension `{end_dim}`. We found "
                f"{len(linkless_start_specs)} linkless specs for window start ({linkless_start_specs}) and "
                f"{len(linkless_end_specs)} linkless specs for window end ({linkless_end_specs})."
            )
            # SCD join targets are joined as dimension links in much the same was as partitions are joined. Therefore,
            # we treat this like a partition time column join and take the dimension spec with the shortest set of
            # entity links so that the subquery uses the correct reference in the ON statement
            start_specs = sorted(start_specs, key=lambda x: len(x.entity_links))
            end_specs = sorted(end_specs, key=lambda x: len(x.entity_links))
            semantic_model_to_window[semantic_model_reference] = ValidityWindowJoinDescription(
                window_start_dimension=start_specs[0], window_end_dimension=end_specs[0]
            )

        assert len(semantic_model_to_window) < 2, (
            f"Found more than 1 set of validity window specs in the input instance set. This is not currently "
            f"supported, as joins between SCD semantic models are not yet allowed! {semantic_model_to_window}"
        )

        if semantic_model_to_window:
            return list(semantic_model_to_window.values())[0]

        return None


class FilterLinkableInstancesWithLeadingLink(InstanceSetTransform[InstanceSet]):
    """Return an instance set with the elements that have a specified leading link removed.

    e.g. Remove "listing__country" if the specified link is "listing".
    """

    def __init__(self, entity_link: EntityReference) -> None:
        """Remove elements with this link as the first element in "entity_links"."""
        self._entity_link = entity_link

    def _should_pass(self, linkable_spec: LinkableInstanceSpec) -> bool:
        if len(linkable_spec.entity_links) == 0:
            return not linkable_spec.reference == self._entity_link
        return linkable_spec.entity_links[0] != self._entity_link

    def transform(self, instance_set: InstanceSet) -> InstanceSet:  # noqa: D102
        # Normal to not filter anything if the instance set has no instances with links.
        filtered_dimension_instances = tuple(x for x in instance_set.dimension_instances if self._should_pass(x.spec))
        filtered_time_dimension_instances = tuple(
            x for x in instance_set.time_dimension_instances if self._should_pass(x.spec)
        )
        filtered_entity_instances = tuple(x for x in instance_set.entity_instances if self._should_pass(x.spec))
        filtered_group_by_metric_instances = tuple(
            x for x in instance_set.group_by_metric_instances if self._should_pass(x.spec)
        )

        output = InstanceSet(
            simple_metric_input_instances=instance_set.simple_metric_input_instances,
            dimension_instances=filtered_dimension_instances,
            time_dimension_instances=filtered_time_dimension_instances,
            entity_instances=filtered_entity_instances,
            group_by_metric_instances=filtered_group_by_metric_instances,
            metric_instances=instance_set.metric_instances,
            metadata_instances=instance_set.metadata_instances,
        )
        return output


class FilterElements(InstanceSetTransform[InstanceSet]):
    """Return an instance set with the elements that don't match any of the pass specs removed."""

    def __init__(
        self,
        include_specs: Optional[InstanceSpecSet] = None,
        exclude_specs: Optional[InstanceSpecSet] = None,
    ) -> None:
        """Constructor.

        Args:
            include_specs: If specified, pass only instances matching these specs.
            exclude_specs: If specified, pass only instances not matching these specs.
        """
        assert_exactly_one_arg_set(include_specs=include_specs, exclude_specs=exclude_specs)
        self._include_specs = include_specs
        self._exclude_specs = exclude_specs

    def _should_pass(self, element_spec: InstanceSpec) -> bool:
        # TODO: Use better matching function
        if self._include_specs:
            return any(x == element_spec for x in self._include_specs.all_specs)
        elif self._exclude_specs:
            return not any(x == element_spec for x in self._exclude_specs.all_specs)
        assert False

    def transform(self, instance_set: InstanceSet) -> InstanceSet:  # noqa: D102
        # Sanity check to make sure the specs are in the instance set
        available_specs = instance_set.spec_set.all_specs
        if self._include_specs:
            include_specs_not_found = []
            for include_spec in self._include_specs.all_specs:
                if include_spec not in available_specs:
                    include_specs_not_found.append(include_spec)
            if include_specs_not_found:
                raise RuntimeError(
                    LazyFormat(
                        "Include specs are not in the spec set - check if this node was constructed correctly.",
                        include_specs_not_found=include_specs_not_found,
                        available_specs=available_specs,
                    )
                )
        elif self._exclude_specs:
            exclude_specs_not_found = []

            for exclude_spec in self._exclude_specs.all_specs:
                if exclude_spec not in available_specs:
                    exclude_specs_not_found.append(exclude_spec)
            if exclude_specs_not_found:
                raise RuntimeError(
                    LazyFormat(
                        "Exclude specs are not in the spec set - check if this node was constructed correctly.",
                        exclude_specs_not_found=exclude_specs_not_found,
                        available_specs=available_specs,
                    )
                )
        else:
            assert False, "Include specs or exclude specs should have been specified."

        output = InstanceSet(
            simple_metric_input_instances=tuple(
                x for x in instance_set.simple_metric_input_instances if self._should_pass(x.spec)
            ),
            dimension_instances=tuple(x for x in instance_set.dimension_instances if self._should_pass(x.spec)),
            time_dimension_instances=tuple(
                x for x in instance_set.time_dimension_instances if self._should_pass(x.spec)
            ),
            entity_instances=tuple(x for x in instance_set.entity_instances if self._should_pass(x.spec)),
            group_by_metric_instances=tuple(
                x for x in instance_set.group_by_metric_instances if self._should_pass(x.spec)
            ),
            metric_instances=tuple(x for x in instance_set.metric_instances if self._should_pass(x.spec)),
            metadata_instances=tuple(x for x in instance_set.metadata_instances if self._should_pass(x.spec)),
        )
        return output


class ChangeSimpleMetricInputAggregationState(InstanceSetTransform[InstanceSet]):
    """Returns a new instance set where all simple-metric inputs are set as a different aggregation state."""

    def __init__(self, aggregation_state_changes: Dict[AggregationState, AggregationState]) -> None:
        """Constructor.

        Args:
            aggregation_state_changes: key is old aggregation state, value is the new aggregation state.
        """
        self._aggregation_state_changes = aggregation_state_changes

    def transform(self, instance_set: InstanceSet) -> InstanceSet:  # noqa: D102
        for instance in instance_set.simple_metric_input_instances:
            assert instance.aggregation_state in self._aggregation_state_changes, (
                f"Aggregation state: {instance.aggregation_state} not handled in change dict: "
                f"{self._aggregation_state_changes}"
            )

        # Copy the simple-metric inputs, but just change the aggregation state to COMPLETE.
        instances = tuple(
            SimpleMetricInputInstance(
                associated_columns=x.associated_columns,
                defined_from=x.defined_from,
                aggregation_state=self._aggregation_state_changes[x.aggregation_state],
                spec=x.spec,
            )
            for x in instance_set.simple_metric_input_instances
        )
        return InstanceSet(
            simple_metric_input_instances=instances,
            dimension_instances=instance_set.dimension_instances,
            time_dimension_instances=instance_set.time_dimension_instances,
            entity_instances=instance_set.entity_instances,
            group_by_metric_instances=instance_set.group_by_metric_instances,
            metric_instances=instance_set.metric_instances,
            metadata_instances=instance_set.metadata_instances,
        )


class UpdateSimpleMetricInputFillNullsWith(InstanceSetTransform[InstanceSet]):
    """Returns a new instance set where all simple-metric inputs have been assigned the fill nulls with property."""

    def __init__(self, null_fill_value_mapping: NullFillValueMapping):
        """Initializer stores the input specs, which contain the fill_nulls_with for each simple-metric input."""
        self._null_fill_value_mapping = null_fill_value_mapping

    def _update_fill_nulls_with(
        self, instances: Tuple[SimpleMetricInputInstance, ...]
    ) -> Tuple[SimpleMetricInputInstance, ...]:
        """Update all simple-metric input instances with the corresponding fill_nulls_with value."""
        updated_instances: List[SimpleMetricInputInstance] = []
        for instance in instances:
            spec = instance.spec
            mapped_spec = self._null_fill_value_mapping.null_fill_value_spec(spec)
            if mapped_spec is not None:
                updated_instances.append(
                    SimpleMetricInputInstance(
                        associated_columns=instance.associated_columns,
                        spec=mapped_spec,
                        aggregation_state=instance.aggregation_state,
                        defined_from=instance.defined_from,
                    )
                )
            else:
                updated_instances.append(instance)

        return tuple(updated_instances)

    def transform(self, instance_set: InstanceSet) -> InstanceSet:  # noqa: D102
        return InstanceSet(
            simple_metric_input_instances=self._update_fill_nulls_with(instance_set.simple_metric_input_instances),
            dimension_instances=instance_set.dimension_instances,
            time_dimension_instances=instance_set.time_dimension_instances,
            entity_instances=instance_set.entity_instances,
            group_by_metric_instances=instance_set.group_by_metric_instances,
            metric_instances=instance_set.metric_instances,
            metadata_instances=instance_set.metadata_instances,
        )


class AliasAggregatedSimpleMetricInputs(InstanceSetTransform[InstanceSet]):
    """Returns a new instance set where all simple-metric inputs have been assigned an alias spec."""

    def __init__(self, alias_mapping: InstanceAliasMapping) -> None:
        """Initializer stores the input specs, which contain the aliases for each simple-metric input.

        Note this class only works if used in conjunction with an `AggregateSimpleMetricInputsNode` that has been generated
        by querying a single semantic model for a single set of aggregated simple-metric inputs. This is currently enforced
        by the structure of the `DataflowPlanBuilder`, which ensures each `AggregateSimpleMetricInputsNode` corresponds to
        a single semantic model set of simple-metric inputs for a single metric, and that these outputs will then be
        combined via joins.
        """
        self._alias_mapping = alias_mapping

    def _alias_simple_metric_input_instances(
        self, instances: Tuple[SimpleMetricInputInstance, ...]
    ) -> Tuple[SimpleMetricInputInstance, ...]:
        """Update all simple-metric input instances with aliases, if any are found in the input spec set."""
        aliased_instances: List[SimpleMetricInputInstance] = []
        for instance in instances:
            spec = instance.spec
            aliased_spec = self._alias_mapping.aliased_spec(spec)
            if aliased_spec is not None:
                aliased_instances.append(
                    SimpleMetricInputInstance(
                        associated_columns=instance.associated_columns,
                        spec=aliased_spec,
                        aggregation_state=instance.aggregation_state,
                        defined_from=instance.defined_from,
                    )
                )
            else:
                aliased_instances.append(instance)

        return tuple(aliased_instances)

    def transform(self, instance_set: InstanceSet) -> InstanceSet:  # noqa: D102
        return InstanceSet(
            simple_metric_input_instances=self._alias_simple_metric_input_instances(
                instance_set.simple_metric_input_instances
            ),
            dimension_instances=instance_set.dimension_instances,
            time_dimension_instances=instance_set.time_dimension_instances,
            entity_instances=instance_set.entity_instances,
            group_by_metric_instances=instance_set.group_by_metric_instances,
            metric_instances=instance_set.metric_instances,
            metadata_instances=instance_set.metadata_instances,
        )


class AddMetrics(InstanceSetTransform[InstanceSet]):
    """Adds the given metric instances to the instance set."""

    def __init__(self, metric_instances: List[MetricInstance]) -> None:  # noqa: D107
        self._metric_instances = metric_instances

    def transform(self, instance_set: InstanceSet) -> InstanceSet:  # noqa: D102
        return InstanceSet(
            simple_metric_input_instances=instance_set.simple_metric_input_instances,
            dimension_instances=instance_set.dimension_instances,
            time_dimension_instances=instance_set.time_dimension_instances,
            entity_instances=instance_set.entity_instances,
            group_by_metric_instances=instance_set.group_by_metric_instances,
            metric_instances=instance_set.metric_instances + tuple(self._metric_instances),
            metadata_instances=instance_set.metadata_instances,
        )


class AddGroupByMetric(InstanceSetTransform[InstanceSet]):
    """Adds the given metric instances to the instance set."""

    def __init__(self, group_by_metric_instance: GroupByMetricInstance) -> None:  # noqa: D107
        self._group_by_metric_instance = group_by_metric_instance

    def transform(self, instance_set: InstanceSet) -> InstanceSet:  # noqa: D102
        return InstanceSet(
            simple_metric_input_instances=instance_set.simple_metric_input_instances,
            dimension_instances=instance_set.dimension_instances,
            time_dimension_instances=instance_set.time_dimension_instances,
            entity_instances=instance_set.entity_instances,
            group_by_metric_instances=instance_set.group_by_metric_instances + (self._group_by_metric_instance,),
            metric_instances=instance_set.metric_instances,
            metadata_instances=instance_set.metadata_instances,
        )


class RemoveSimpleMetricInputTransform(InstanceSetTransform[InstanceSet]):
    """Remove simple-metric inputs from the instance set."""

    def transform(self, instance_set: InstanceSet) -> InstanceSet:  # noqa: D102
        return InstanceSet(
            simple_metric_input_instances=(),
            dimension_instances=instance_set.dimension_instances,
            time_dimension_instances=instance_set.time_dimension_instances,
            entity_instances=instance_set.entity_instances,
            group_by_metric_instances=instance_set.group_by_metric_instances,
            metric_instances=instance_set.metric_instances,
            metadata_instances=instance_set.metadata_instances,
        )


class RemoveMetrics(InstanceSetTransform[InstanceSet]):
    """Remove metrics from the instance set."""

    def transform(self, instance_set: InstanceSet) -> InstanceSet:  # noqa: D102
        return InstanceSet(
            simple_metric_input_instances=instance_set.simple_metric_input_instances,
            dimension_instances=instance_set.dimension_instances,
            time_dimension_instances=instance_set.time_dimension_instances,
            entity_instances=instance_set.entity_instances,
            group_by_metric_instances=instance_set.group_by_metric_instances,
            metric_instances=(),
            metadata_instances=instance_set.metadata_instances,
        )


class CreateSqlColumnReferencesForInstances(InstanceSetTransform[Tuple[SqlColumnReferenceExpression, ...]]):
    """Create select column expressions that will express all instances in the set.

    It assumes that the column names of the instances are represented by the supplied column association resolver and
    come from the given table alias.
    """

    def __init__(
        self,
        table_alias: str,
        column_resolver: ColumnAssociationResolver,
    ) -> None:
        """Initializer.

        Args:
            table_alias: the table alias to select columns from
            column_resolver: resolver to name columns.
        """
        self._table_alias = table_alias
        self._column_resolver = column_resolver

    def transform(self, instance_set: InstanceSet) -> Tuple[SqlColumnReferenceExpression, ...]:  # noqa: D102
        column_names = [
            self._column_resolver.resolve_spec(spec).column_name for spec in instance_set.spec_set.all_specs
        ]
        return tuple(
            SqlColumnReferenceExpression.create(
                SqlColumnReference(
                    table_alias=self._table_alias,
                    column_name=column_name,
                ),
            )
            for column_name in column_names
        )


class CreateSelectColumnForCombineOutputNode(InstanceSetTransform[SelectColumnSet]):
    """Create select column expressions for the instance for joining outputs.

    It assumes that the column names of the instances are represented by the supplied column association resolver and
    come from the given table alias.
    """

    def __init__(  # noqa: D107
        self,
        table_alias: str,
        column_resolver: ColumnAssociationResolver,
        metric_lookup: MetricLookup,
    ) -> None:
        self._table_alias = table_alias
        self._column_resolver = column_resolver
        self._metric_lookup = metric_lookup

    def _create_select_column(self, spec: InstanceSpec, fill_nulls_with: Optional[int] = None) -> SqlSelectColumn:
        """Creates the select column for the given spec and the fill value."""
        column_name = self._column_resolver.resolve_spec(spec).column_name
        column_reference_expression = SqlColumnReferenceExpression.create(
            col_ref=SqlColumnReference(
                table_alias=self._table_alias,
                column_name=column_name,
            )
        )
        select_expression: SqlExpressionNode = SqlFunctionExpression.build_expression_from_aggregation_type(
            aggregation_type=AggregationType.MAX, sql_column_expression=column_reference_expression
        )
        if fill_nulls_with is not None:
            select_expression = SqlAggregateFunctionExpression.create(
                sql_function=SqlFunction.COALESCE,
                sql_function_args=[
                    select_expression,
                    SqlStringExpression.create(str(fill_nulls_with)),
                ],
            )
        return SqlSelectColumn(
            expr=select_expression,
            column_alias=column_name,
        )

    def _create_select_columns_for_metrics(self, metric_instances: Tuple[MetricInstance, ...]) -> List[SqlSelectColumn]:
        select_columns: List[SqlSelectColumn] = []
        for metric_instance in metric_instances:
            metric_reference = MetricReference(element_name=metric_instance.defined_from.metric_name)
            metric = self._metric_lookup.get_metric(metric_reference)
            metric_type = metric.type
            if metric_type is MetricType.SIMPLE:
                fill_nulls_with = metric.type_params.fill_nulls_with
            elif (
                metric_type is MetricType.RATIO
                or metric_type is MetricType.DERIVED
                or metric_type is MetricType.CUMULATIVE
                or metric_type is MetricType.CONVERSION
            ):
                fill_nulls_with = None
            else:
                assert_values_exhausted(metric_type)
            select_columns.append(
                self._create_select_column(spec=metric_instance.spec, fill_nulls_with=fill_nulls_with)
            )
        return select_columns

    def _create_select_columns_for_simple_metric_inputs(
        self, instances: Tuple[SimpleMetricInputInstance, ...]
    ) -> List[SqlSelectColumn]:
        select_columns: List[SqlSelectColumn] = []
        for instance in instances:
            simple_metric_input_spec = instance.spec
            select_columns.append(
                self._create_select_column(
                    spec=simple_metric_input_spec, fill_nulls_with=simple_metric_input_spec.fill_nulls_with
                )
            )
        return select_columns

    def transform(self, instance_set: InstanceSet) -> SelectColumnSet:  # noqa: D102
        return SelectColumnSet.create(
            metric_columns=self._create_select_columns_for_metrics(instance_set.metric_instances),
            simple_metric_input_columns=self._create_select_columns_for_simple_metric_inputs(
                instance_set.simple_metric_input_instances
            ),
        )


# TODO: delete this class & all uses. It doesn't do anything.
class ChangeAssociatedColumns(InstanceSetTransform[InstanceSet]):
    """Change the columns associated with instances to the one specified by the resolver.

    This is useful for conveniently generating output instances for a node that serve as a "pass-through". The output
    instances can be a copy of the parent's instances, except that the column names need to be changed.

    e.g. the parent may have a data set:
        sql:
            SELECT
                is_lux AS is_lux_latext
            ...
        instance:
            DimensionInstance(column_name="is_lux", ...)

    but for the current node, we want a data set like:

        sql:
            SELECT
                is_lux_latest
                ...
            FROM (
                -- SQL from parent
                is_lux AS is_lux_latest
                ...
            )
            ...
        instance:
            DimensionInstance(column_name="is_lux_latest")
    """

    def __init__(self, column_association_resolver: ColumnAssociationResolver) -> None:  # noqa: D107
        self._column_association_resolver = column_association_resolver

    def transform(self, instance_set: InstanceSet) -> InstanceSet:  # noqa: D102
        output_simple_metric_input_instances = []
        for input_simple_metric_input_instance in instance_set.simple_metric_input_instances:
            output_simple_metric_input_instances.append(
                SimpleMetricInputInstance(
                    associated_columns=(
                        self._column_association_resolver.resolve_spec(input_simple_metric_input_instance.spec),
                    ),
                    spec=input_simple_metric_input_instance.spec,
                    defined_from=input_simple_metric_input_instance.defined_from,
                    aggregation_state=input_simple_metric_input_instance.aggregation_state,
                )
            )

        output_dimension_instances = []
        for input_dimension_instance in instance_set.dimension_instances:
            output_dimension_instances.append(
                DimensionInstance(
                    associated_columns=(self._column_association_resolver.resolve_spec(input_dimension_instance.spec),),
                    spec=input_dimension_instance.spec,
                    defined_from=input_dimension_instance.defined_from,
                )
            )

        output_time_dimension_instances = []
        for input_time_dimension_instance in instance_set.time_dimension_instances:
            output_time_dimension_instances.append(
                TimeDimensionInstance(
                    associated_columns=(
                        self._column_association_resolver.resolve_spec(input_time_dimension_instance.spec),
                    ),
                    spec=input_time_dimension_instance.spec,
                    defined_from=input_time_dimension_instance.defined_from,
                )
            )

        output_entity_instances = []
        for input_entity_instance in instance_set.entity_instances:
            output_entity_instances.append(
                EntityInstance(
                    associated_columns=(self._column_association_resolver.resolve_spec(input_entity_instance.spec),),
                    spec=input_entity_instance.spec,
                    defined_from=input_entity_instance.defined_from,
                )
            )

        output_metric_instances = []
        for input_metric_instance in instance_set.metric_instances:
            output_metric_instances.append(
                MetricInstance(
                    associated_columns=(self._column_association_resolver.resolve_spec(input_metric_instance.spec),),
                    spec=input_metric_instance.spec,
                    defined_from=input_metric_instance.defined_from,
                )
            )

        output_metadata_instances = []
        for input_metadata_instance in instance_set.metadata_instances:
            output_metadata_instances.append(
                MetadataInstance(
                    associated_columns=(self._column_association_resolver.resolve_spec(input_metadata_instance.spec),),
                    spec=input_metadata_instance.spec,
                )
            )

        output_group_by_metric_instances = []
        for input_group_by_metric_instance in instance_set.group_by_metric_instances:
            output_group_by_metric_instances.append(
                GroupByMetricInstance(
                    associated_columns=(
                        self._column_association_resolver.resolve_spec(input_group_by_metric_instance.spec),
                    ),
                    spec=input_group_by_metric_instance.spec,
                    defined_from=input_group_by_metric_instance.defined_from,
                )
            )
        return InstanceSet(
            simple_metric_input_instances=tuple(output_simple_metric_input_instances),
            dimension_instances=tuple(output_dimension_instances),
            time_dimension_instances=tuple(output_time_dimension_instances),
            entity_instances=tuple(output_entity_instances),
            group_by_metric_instances=tuple(output_group_by_metric_instances),
            metric_instances=tuple(output_metric_instances),
            metadata_instances=tuple(output_metadata_instances),
        )


class ConvertToMetadata(InstanceSetTransform[InstanceSet]):
    """Removes all instances from old instance set and replaces them with a set of metadata instances."""

    def __init__(self, metadata_instances: Sequence[MetadataInstance]) -> None:  # noqa: D107
        self._metadata_instances = metadata_instances

    def transform(self, instance_set: InstanceSet) -> InstanceSet:  # noqa: D102
        return InstanceSet(
            metadata_instances=tuple(self._metadata_instances),
        )


class AddMetadata(InstanceSetTransform[InstanceSet]):
    """Adds the given metric instances to the instance set."""

    def __init__(self, metadata_instances: Sequence[MetadataInstance]) -> None:  # noqa: D107
        self._metadata_instances = metadata_instances

    def transform(self, instance_set: InstanceSet) -> InstanceSet:  # noqa: D102
        return InstanceSet(
            simple_metric_input_instances=instance_set.simple_metric_input_instances,
            dimension_instances=instance_set.dimension_instances,
            time_dimension_instances=instance_set.time_dimension_instances,
            entity_instances=instance_set.entity_instances,
            group_by_metric_instances=instance_set.group_by_metric_instances,
            metric_instances=instance_set.metric_instances,
            metadata_instances=instance_set.metadata_instances + tuple(self._metadata_instances),
        )
