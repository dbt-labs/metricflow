from __future__ import annotations

import datetime
import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter
from dbt_semantic_interfaces.pretty_print import pformat_big_objects
from dbt_semantic_interfaces.protocols.dimension import DimensionType
from dbt_semantic_interfaces.protocols.metric import MetricType
from dbt_semantic_interfaces.protocols.where_filter import WhereFilter
from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    MetricReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.dataflow_plan import BaseOutput
from metricflow.dataset.dataset import DataSet
from metricflow.dataset.semantic_model_adapter import SemanticModelDataSet
from metricflow.errors.errors import UnableToSatisfyQueryError
from metricflow.filters.time_constraint import TimeRangeConstraint
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow.query.query_exceptions import InvalidQueryException
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.specs import (
    DimensionSpec,
    EntitySpec,
    LinkableInstanceSpec,
    LinkableSpecSet,
    MetricFlowQuerySpec,
    MetricSpec,
    OrderBySpec,
    TimeDimensionSpec,
    WhereFilterSpec,
)
from metricflow.specs.where_filter_transform import WhereSpecFactory
from metricflow.time.time_granularity_solver import (
    PartialTimeDimensionSpec,
    RequestTimeGranularityException,
    TimeGranularitySolver,
)

logging.captureWarnings(True)
import rapidfuzz.fuzz  # noqa: E402
import rapidfuzz.process  # noqa: E402

logging.captureWarnings(False)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class QueryTimeLinkableSpecSet:
    """Linkable specs that are specified at query time.

    This is different from LinkableSpecSet in that it allows for partially specified time dimensions. e.g. in a query,
    metric_time might be specified without a granularity suffix, but means the lowest possible time granularity.
    """

    dimension_specs: Tuple[DimensionSpec, ...]
    time_dimension_specs: Tuple[TimeDimensionSpec, ...]
    partial_time_dimension_specs: Tuple[PartialTimeDimensionSpec, ...]
    entity_specs: Tuple[EntitySpec, ...]

    @staticmethod
    def create_from_linkable_spec_set(linkable_spec_set: LinkableSpecSet) -> QueryTimeLinkableSpecSet:  # noqa: D
        return QueryTimeLinkableSpecSet(
            dimension_specs=linkable_spec_set.dimension_specs,
            time_dimension_specs=linkable_spec_set.time_dimension_specs,
            partial_time_dimension_specs=(),
            entity_specs=linkable_spec_set.entity_specs,
        )

    @staticmethod
    def combine(spec_sets: Sequence[QueryTimeLinkableSpecSet]) -> QueryTimeLinkableSpecSet:  # noqa: D
        return QueryTimeLinkableSpecSet(
            dimension_specs=tuple(
                dimension_spec for spec_set in spec_sets for dimension_spec in spec_set.dimension_specs
            ),
            time_dimension_specs=tuple(
                time_dimension_spec for spec_set in spec_sets for time_dimension_spec in spec_set.time_dimension_specs
            ),
            partial_time_dimension_specs=tuple(
                partial_time_dimension_spec
                for spec_set in spec_sets
                for partial_time_dimension_spec in spec_set.partial_time_dimension_specs
            ),
            entity_specs=tuple(entity_spec for spec_set in spec_sets for entity_spec in spec_set.entity_specs),
        )


class MetricFlowQueryParser:
    """Parse input strings from the user into a metric query specification.

    Definitions:
    element name - the name of an element (measure, dimension, entity) in a semantic model, or a metric name.
    qualified name - an element name with prefixes and suffixes added to it that further describe transformations or
    conditions for the element to retrieve. e.g. "ds__month" is the "ds" time dimension at the "month" granularity. Or
    "user_id__country" is the "country" dimension that is retrieved by joining "user_id" to the measure semantic model.
    """

    def __init__(  # noqa: D
        self,
        column_association_resolver: ColumnAssociationResolver,
        model: SemanticManifestLookup,
        source_nodes: Sequence[BaseOutput[SemanticModelDataSet]],
        node_output_resolver: DataflowPlanNodeOutputDataSetResolver[SemanticModelDataSet],
    ) -> None:
        self._column_association_resolver = column_association_resolver
        self._model = model
        self._metric_lookup = model.metric_lookup
        self._semantic_model_lookup = model.semantic_model_lookup

        # Set up containers for known element names
        self._known_entity_element_references = self._semantic_model_lookup.get_entity_references()

        self._known_time_dimension_element_references = [DataSet.metric_time_dimension_reference()]
        self._known_dimension_element_references = []
        for dimension_reference in self._semantic_model_lookup.get_dimension_references():
            dimension = self._semantic_model_lookup.get_dimension(dimension_reference)
            if dimension.type == DimensionType.CATEGORICAL:
                self._known_dimension_element_references.append(dimension_reference)
            elif dimension.type == DimensionType.TIME:
                self._known_time_dimension_element_references.append(dimension_reference.time_dimension_reference)
            else:
                raise RuntimeError(f"Unhandled linkable type: {dimension.type}")

        self._known_metric_names = set(self._metric_lookup.metric_references)
        self._metric_time_dimension_reference = DataSet.metric_time_dimension_reference()
        self._time_granularity_solver = TimeGranularitySolver(
            semantic_manifest_lookup=self._model,
            source_nodes=source_nodes,
            node_output_resolver=node_output_resolver,
        )

    @staticmethod
    def _top_fuzzy_matches(
        item: str,
        candidate_items: Sequence[str],
        max_suggestions: int = 6,
        min_score: int = 50,
    ) -> Sequence[str]:
        """Return the top items (by edit distance) in candidate_items that fuzzy matches the given item."""
        # Rank choices by edit distance score.
        # extract() return a tuple like (name, score)
        top_ranked_suggestions = sorted(
            rapidfuzz.process.extract(
                # This scorer seems to return the best results.
                item,
                list(candidate_items),
                limit=max_suggestions,
                scorer=rapidfuzz.fuzz.token_set_ratio,
            ),
            # Put the highest scoring item at the top of the list.
            key=lambda x: x[1],
            reverse=True,
        )
        # If the scores are too low, then it looks nonsensical, so remove those.
        top_ranked_suggestions = [x for x in top_ranked_suggestions if x[1] > min_score]
        return [x[0] for x in top_ranked_suggestions]

    def parse_and_validate_query(
        self,
        metric_names: Sequence[str],
        group_by_names: Sequence[str],
        limit: Optional[int] = None,
        time_constraint_start: Optional[datetime.datetime] = None,
        time_constraint_end: Optional[datetime.datetime] = None,
        where_constraint: Optional[WhereFilter] = None,
        where_constraint_str: Optional[str] = None,
        order: Optional[Sequence[str]] = None,
        time_granularity: Optional[TimeGranularity] = None,
    ) -> MetricFlowQuerySpec:
        """Parse the query into spec objects, validating them in the process.

        e.g. make sure that the given metric is a valid metric name.
        """
        start_time = time.time()
        try:
            return self._parse_and_validate_query(
                metric_names=metric_names,
                group_by_names=group_by_names,
                limit=limit,
                time_constraint_start=time_constraint_start,
                time_constraint_end=time_constraint_end,
                where_constraint=where_constraint,
                where_constraint_str=where_constraint_str,
                order=order,
                time_granularity=time_granularity,
            )
        finally:
            logger.info(f"Parsing the query took: {time.time() - start_time:.2f}s")

    def _validate_no_time_dimension_query(self, metric_references: Sequence[MetricReference]) -> None:
        """Validate if all requested metrics are queryable without a time dimension."""
        for metric_reference in metric_references:
            metric = self._metric_lookup.get_metric(metric_reference)
            if metric.type == MetricType.CUMULATIVE:
                # Cumulative metrics configured with a window/grain_to_date cannot be queried without a dimension.
                if metric.type_params.window or metric.type_params.grain_to_date:
                    raise UnableToSatisfyQueryError(
                        f"Metric {metric.name} is a cumulative metric specified with a window/grain_to_date "
                        f"which must be queried with the dimension 'metric_time'.",
                    )
            elif metric.type == MetricType.DERIVED:
                for input_metric in metric.type_params.metrics or []:
                    if input_metric.offset_window or input_metric.offset_to_grain:
                        raise UnableToSatisfyQueryError(
                            f"Metric '{metric.name}' is a derived metric that contains input metrics with "
                            "an `offset_window` or `offset_to_grain` which must be queried with the "
                            "dimension 'metric_time'."
                        )

    def _validate_linkable_specs(
        self,
        metric_references: Tuple[MetricReference, ...],
        all_linkable_specs: QueryTimeLinkableSpecSet,
        time_dimension_specs: Tuple[TimeDimensionSpec, ...],
    ) -> None:
        invalid_group_bys = self._get_invalid_linkable_specs(
            metric_references=metric_references,
            dimension_specs=all_linkable_specs.dimension_specs,
            time_dimension_specs=time_dimension_specs,
            entity_specs=all_linkable_specs.entity_specs,
        )
        if len(invalid_group_bys) > 0:
            valid_group_by_names_for_metrics = sorted(
                x.qualified_name
                for x in self._metric_lookup.element_specs_for_metrics(metric_references=list(metric_references))
            )
            # Create suggestions for invalid dimensions in case the user made a typo.
            suggestion_sections = {}
            for invalid_group_by in invalid_group_bys:
                suggestions = MetricFlowQueryParser._top_fuzzy_matches(
                    item=invalid_group_by.qualified_name, candidate_items=valid_group_by_names_for_metrics
                )
                section_key = f"Suggestions for invalid dimension '{invalid_group_by.qualified_name}'"
                section_value = pformat_big_objects(suggestions)
                suggestion_sections[section_key] = section_value
            raise UnableToSatisfyQueryError(
                f"Dimensions {[x.qualified_name for x in invalid_group_bys]} cannot be "
                f"resolved for metrics {[x.element_name for x in metric_references]}. The invalid dimension may not "
                f"exist, require an ambiguous join (e.g. a join path that can be satisfied in multiple ways), "
                f"or require a fanout join.",
                context=suggestion_sections,
            )

    def _construct_metric_specs_for_query(
        self, metric_references: Tuple[MetricReference, ...]
    ) -> Tuple[MetricSpec, ...]:
        """Populate MetricSpecs.

        Construct MetricSpecs in the preaggregated state to pass into the DataflowPlanBuilder.

        NOTE: Currently, we populate only the metrics provided in the query, but with derived metrics,
        there are nested metrics that do not get populated here, but rather in the MetricSemantic during
        the builder process. This is a process that should be refined altogther.
        """
        metric_specs = []
        for metric_reference in metric_references:
            metric = self._metric_lookup.get_metric(metric_reference)
            metric_where_constraint: Optional[WhereFilterSpec] = None
            if metric.filter:
                # add constraint to MetricSpec
                metric_where_constraint = WhereSpecFactory(
                    column_association_resolver=self._column_association_resolver,
                ).create_from_where_filter(metric.filter)
            # TODO: Directly initializing Spec object instead of using a factory method since
            #       importing WhereConstraintConverter is a problem in specs.py
            metric_specs.append(
                MetricSpec(
                    element_name=metric_reference.element_name,
                    constraint=metric_where_constraint,
                )
            )
        return tuple(metric_specs)

    def _parse_and_validate_query(
        self,
        metric_names: Sequence[str],
        group_by_names: Sequence[str],
        limit: Optional[int] = None,
        time_constraint_start: Optional[datetime.datetime] = None,
        time_constraint_end: Optional[datetime.datetime] = None,
        where_constraint: Optional[WhereFilter] = None,
        where_constraint_str: Optional[str] = None,
        order: Optional[Sequence[str]] = None,
        time_granularity: Optional[TimeGranularity] = None,
    ) -> MetricFlowQuerySpec:
        assert not (
            where_constraint and where_constraint_str
        ), "Both where_constraint and where_constraint_str should not be set"

        where_filter: Optional[WhereFilter]
        if where_constraint_str:
            where_filter = PydanticWhereFilter(where_sql_template=where_constraint_str)
        else:
            where_filter = where_constraint

        # Get metric references used for validations
        # In a case of derived metric, all the input metrics would be here.
        metric_references = self._parse_metric_names(metric_names)
        if time_constraint_start is None:
            time_constraint_start = TimeRangeConstraint.ALL_TIME_BEGIN()
        elif time_constraint_start < TimeRangeConstraint.ALL_TIME_BEGIN():
            logger.warning(
                f"Start time for the supplied time constraint {time_constraint_start.isoformat()} is <less> than the "
                f"minimum allowed ('{TimeRangeConstraint.ALL_TIME_BEGIN().isoformat()}'). Changing to the minimum."
            )
            time_constraint_start = TimeRangeConstraint.ALL_TIME_BEGIN()
        elif time_constraint_start > TimeRangeConstraint.ALL_TIME_END():
            logger.warning(
                f"Start time for the supplied time constraint {time_constraint_start.isoformat()} is > than the "
                f"maximum allowed ('{TimeRangeConstraint.ALL_TIME_END().isoformat()}'). Changing to the maximum."
            )
            time_constraint_start = TimeRangeConstraint.ALL_TIME_END()

        if time_constraint_end is None:
            time_constraint_end = TimeRangeConstraint.ALL_TIME_END()
        elif time_constraint_end > TimeRangeConstraint.ALL_TIME_END():
            logger.warning(
                f"End time for the supplied time constraint {time_constraint_end.isoformat()} is > than the "
                f"maximum allowed ('{TimeRangeConstraint.ALL_TIME_END().isoformat()}'). Changing to the maximum."
            )
            time_constraint_end = TimeRangeConstraint.ALL_TIME_END()
        elif time_constraint_end < TimeRangeConstraint.ALL_TIME_BEGIN():
            logger.warning(
                f"End time for the supplied time constraint {time_constraint_end.isoformat()} is less than the "
                f"minimum allowed ('{TimeRangeConstraint.ALL_TIME_BEGIN().isoformat()}'). Changing to the minimum."
            )
            time_constraint_end = TimeRangeConstraint.ALL_TIME_BEGIN()

        if time_constraint_end < time_constraint_start:
            raise ValueError(
                f"End of time constraint {time_constraint_end.isoformat()} is before start of time constraint "
                f"{time_constraint_start.isoformat()}"
            )

        time_constraint: Optional[TimeRangeConstraint] = TimeRangeConstraint(
            start_time=time_constraint_start, end_time=time_constraint_end
        )
        if time_constraint == TimeRangeConstraint.all_time():
            # If the time constraint is all time, just ignore and not render
            time_constraint = None

        requested_linkable_specs = self._parse_linkable_element_names(group_by_names, metric_references)
        where_filter_spec: Optional[WhereFilterSpec] = None
        if where_filter is not None:
            where_filter_spec = WhereSpecFactory(
                column_association_resolver=self._column_association_resolver,
            ).create_from_where_filter(where_filter)

            where_spec_set = QueryTimeLinkableSpecSet.create_from_linkable_spec_set(where_filter_spec.linkable_spec_set)
            requested_linkable_specs_with_requested_filter_specs = QueryTimeLinkableSpecSet.combine(
                (
                    requested_linkable_specs,
                    where_spec_set,
                )
            )
        else:
            requested_linkable_specs_with_requested_filter_specs = requested_linkable_specs

        partial_time_dimension_spec_replacements = (
            self._time_granularity_solver.resolve_granularity_for_partial_time_dimension_specs(
                metric_references=metric_references,
                partial_time_dimension_specs=requested_linkable_specs.partial_time_dimension_specs,
                metric_time_dimension_reference=self._metric_time_dimension_reference,
                time_granularity=time_granularity,
            )
        )

        time_dimension_specs = requested_linkable_specs.time_dimension_specs + tuple(
            time_dimension_spec for _, time_dimension_spec in partial_time_dimension_spec_replacements.items()
        )
        if len(time_dimension_specs) == 0:
            self._validate_no_time_dimension_query(metric_references=metric_references)

        self._time_granularity_solver.validate_time_granularity(metric_references, time_dimension_specs)

        order_by_specs = self._parse_order_by(order or [], partial_time_dimension_spec_replacements)

        # For each metric, verify that it's possible to retrieve all group by elements, including the ones as required
        # by the filters.
        # TODO: Consider moving this logic into _validate_linkable_specs().
        for metric_reference in metric_references:
            metric = self._metric_lookup.get_metric(metric_reference)
            if metric.filter is not None:
                group_by_specs_for_one_metric = self._parse_linkable_element_names(
                    qualified_linkable_names=group_by_names,
                    metric_references=(metric_reference,),
                )

                # Combine the group by elements from the query with the group by elements that are required by the
                # metric filter to see if that's a valid set that could be queried.
                self._validate_linkable_specs(
                    metric_references=(metric_reference,),
                    all_linkable_specs=QueryTimeLinkableSpecSet.combine(
                        (
                            group_by_specs_for_one_metric,
                            QueryTimeLinkableSpecSet.create_from_linkable_spec_set(
                                (
                                    WhereSpecFactory(
                                        column_association_resolver=self._column_association_resolver
                                    ).create_from_where_filter(metric.filter)
                                ).linkable_spec_set
                            ),
                        ),
                    ),
                    time_dimension_specs=time_dimension_specs,
                )

        # Validate all of them together.
        self._validate_linkable_specs(
            metric_references=metric_references,
            all_linkable_specs=requested_linkable_specs_with_requested_filter_specs,
            time_dimension_specs=time_dimension_specs,
        )

        self._validate_order_by_specs(
            order_by_specs=order_by_specs,
            metric_references=metric_references,
            linkable_specs=LinkableSpecSet(
                dimension_specs=requested_linkable_specs.dimension_specs,
                time_dimension_specs=time_dimension_specs,
                entity_specs=requested_linkable_specs.entity_specs,
            ),
        )

        # Update constraints to be appropriate for the time granularity of the query.
        if time_constraint:
            logger.info(f"Time constraint before adjustment is {time_constraint}")
            time_constraint = self._adjust_time_range_constraint(
                metric_references=metric_references,
                time_dimension_specs=time_dimension_specs,
                time_range_constraint=time_constraint,
            )
            logger.info(f"Time constraint after adjustment is {time_constraint}")

        if limit is not None and limit < 0:
            raise InvalidQueryException(f"Limit was specified as {limit}, which is < 0.")

        if where_filter_spec:
            self._time_granularity_solver.validate_time_granularity(
                metric_references=metric_references,
                time_dimension_specs=where_filter_spec.linkable_spec_set.time_dimension_specs,
            )

        base_metric_references = self._parse_metric_names(metric_names, traverse_metric_inputs=False)
        metric_specs = self._construct_metric_specs_for_query(base_metric_references)

        return MetricFlowQuerySpec(
            metric_specs=metric_specs,
            dimension_specs=requested_linkable_specs.dimension_specs,
            entity_specs=requested_linkable_specs.entity_specs,
            time_dimension_specs=time_dimension_specs,
            order_by_specs=order_by_specs,
            time_range_constraint=time_constraint,
            where_constraint=where_filter_spec,
            limit=limit,
        )

    def _validate_order_by_specs(
        self,
        order_by_specs: Sequence[OrderBySpec],
        metric_references: Sequence[MetricReference],
        linkable_specs: LinkableSpecSet,
    ) -> None:
        """Checks that the order by specs references an item in the query."""
        # TODO: this is a workaround
        # Need to figure out whether we should clean up OrderBySpec or if we have to actually pass a fully resolved MetricSpec here
        metric_specs = [MetricSpec.from_reference(metric_reference) for metric_reference in metric_references]
        for order_by_spec in order_by_specs:
            if not (
                order_by_spec.item in metric_specs
                or order_by_spec.item in linkable_specs.dimension_specs
                or order_by_spec.item in linkable_specs.time_dimension_specs
                or order_by_spec.item in linkable_specs.entity_specs
            ):
                raise InvalidQueryException(f"Order by item {order_by_spec} not in the query")

    def _adjust_time_range_constraint(
        self,
        metric_references: Sequence[MetricReference],
        time_dimension_specs: Sequence[TimeDimensionSpec],
        time_range_constraint: TimeRangeConstraint,
    ) -> TimeRangeConstraint:
        """Adjust the time range constraint so that it matches the boundaries of the granularity of the result."""
        self._time_granularity_solver.validate_time_granularity(metric_references, time_dimension_specs)

        smallest_primary_time_granularity_in_query = self._find_smallest_metric_time_dimension_spec_granularity(
            time_dimension_specs
        )
        if smallest_primary_time_granularity_in_query:
            adjusted_to_granularity = smallest_primary_time_granularity_in_query

        else:
            _, adjusted_to_granularity = self._time_granularity_solver.local_dimension_granularity_range(
                metric_references=metric_references,
                local_time_dimension_reference=self._metric_time_dimension_reference,
            )
        logger.info(f"Adjusted primary time granularity is {adjusted_to_granularity}")
        return self._time_granularity_solver.adjust_time_range_to_granularity(
            time_range_constraint, adjusted_to_granularity
        )

    def _find_replacement_for_metric_time_dimension(
        self, replacements: Dict[PartialTimeDimensionSpec, TimeDimensionSpec]
    ) -> Tuple[PartialTimeDimensionSpec, TimeDimensionSpec]:
        for partial_time_dimension_spec_to_replace, replace_with_time_dimension_spec in replacements.items():
            if (
                partial_time_dimension_spec_to_replace.element_name
                == self._metric_time_dimension_reference.element_name
                and partial_time_dimension_spec_to_replace.entity_links == ()
            ):
                return partial_time_dimension_spec_to_replace, replace_with_time_dimension_spec

        raise RuntimeError(f"Replacement for metric time dimension '{self._metric_time_dimension_reference}' not found")

    def _metric_time_dimension_specified_without_granularity(
        self, partial_time_dimension_specs: Sequence[PartialTimeDimensionSpec]
    ) -> bool:
        # This detects a user query like: "query --metrics monthly_bookings --dimensions ds"
        # The granularity for "ds" is not specified by the user.
        for time_dimension_spec in partial_time_dimension_specs:
            if (
                time_dimension_spec.element_name == self._metric_time_dimension_reference.element_name
                and time_dimension_spec.entity_links == ()
            ):
                return True
        return False

    def _metrics_have_same_time_granularities(self, metric_references: Sequence[MetricReference]) -> bool:
        (
            min_granularity,
            max_granularity,
        ) = self._time_granularity_solver.local_dimension_granularity_range(
            metric_references=metric_references,
            local_time_dimension_reference=self._metric_time_dimension_reference,
        )
        return min_granularity == max_granularity

    def _find_smallest_metric_time_dimension_spec_granularity(
        self, time_dimension_specs: Sequence[TimeDimensionSpec]
    ) -> Optional[TimeGranularity]:
        metric_time_dimension_specs: List[TimeDimensionSpec] = [
            x
            for x in time_dimension_specs
            if (
                x.element_name == self._metric_time_dimension_reference.element_name
                and x.entity_links == ()
                and x.time_granularity
            )
        ]

        metric_time_dimension_specs.sort(key=lambda x: x.time_granularity.to_int())
        if len(metric_time_dimension_specs) > 0:
            return metric_time_dimension_specs[0].time_granularity
        else:
            return None

    def _parse_metric_names(
        self, metric_names: Sequence[str], traverse_metric_inputs: bool = True
    ) -> Tuple[MetricReference, ...]:
        """Converts metric names into metric names. An exception is thrown if the name is invalid."""
        # The config must be lower-case, so we lower case for case-insensitivity against query inputs from the user.
        metric_names = [x.lower() for x in metric_names]

        known_metric_names = set(self._metric_lookup.metric_references)
        metric_references: List[MetricReference] = []
        for metric_name in metric_names:
            metric_reference = MetricReference(element_name=metric_name)
            if metric_reference not in known_metric_names:
                suggestions = {
                    f"Suggestions for '{metric_name}'": pformat_big_objects(
                        MetricFlowQueryParser._top_fuzzy_matches(
                            item=metric_name,
                            candidate_items=[x.element_name for x in self._metric_lookup.metric_references],
                        )
                    )
                }
                raise UnableToSatisfyQueryError(
                    f"Unknown metric: '{metric_name}'",
                    context=suggestions,
                )
            metric_references.append(metric_reference)
            if traverse_metric_inputs:
                metric = self._metric_lookup.get_metric(metric_reference)
                if metric.type == MetricType.DERIVED:
                    input_metrics = self._parse_metric_names([metric.name for metric in metric.input_metrics])
                    metric_references.extend(list(input_metrics))
        return tuple(metric_references)

    def _parse_linkable_element_names(
        self, qualified_linkable_names: Sequence[str], metric_references: Sequence[MetricReference]
    ) -> QueryTimeLinkableSpecSet:
        """Convert the linkable spec names into the respective specification objects."""
        qualified_linkable_names = [x.lower() for x in qualified_linkable_names]

        dimension_specs = []
        time_dimension_specs = []
        partial_time_dimension_specs = []
        entity_specs = []

        for qualified_name in qualified_linkable_names:
            structured_name = StructuredLinkableSpecName.from_name(qualified_name)
            element_name = structured_name.element_name
            entity_links = tuple(EntityReference(element_name=x) for x in structured_name.entity_link_names)
            # Create the spec based on the type of element referenced.
            if TimeDimensionReference(element_name=element_name) in self._known_time_dimension_element_references:
                if structured_name.time_granularity:
                    time_dimension_specs.append(
                        TimeDimensionSpec(
                            element_name=element_name,
                            entity_links=entity_links,
                            time_granularity=structured_name.time_granularity,
                        )
                    )
                else:
                    partial_time_dimension_specs.append(
                        PartialTimeDimensionSpec(
                            element_name=element_name,
                            entity_links=entity_links,
                        )
                    )
            elif DimensionReference(element_name=element_name) in self._known_dimension_element_references:
                dimension_specs.append(DimensionSpec(element_name=element_name, entity_links=entity_links))
            elif EntityReference(element_name=element_name) in self._known_entity_element_references:
                entity_specs.append(EntitySpec(element_name=element_name, entity_links=entity_links))
            else:
                valid_group_by_names_for_metrics = sorted(
                    x.qualified_name for x in self._metric_lookup.element_specs_for_metrics(list(metric_references))
                )

                suggestions = {
                    f"Suggestions for '{qualified_name}'": pformat_big_objects(
                        MetricFlowQueryParser._top_fuzzy_matches(
                            item=qualified_name,
                            candidate_items=valid_group_by_names_for_metrics,
                        )
                    )
                }
                raise UnableToSatisfyQueryError(
                    f"Unknown element name '{element_name}' in dimension name '{qualified_name}'",
                    context=suggestions,
                )

        return QueryTimeLinkableSpecSet(
            dimension_specs=tuple(dimension_specs),
            time_dimension_specs=tuple(time_dimension_specs),
            partial_time_dimension_specs=tuple(partial_time_dimension_specs),
            entity_specs=tuple(entity_specs),
        )

    def _get_invalid_linkable_specs(
        self,
        metric_references: Tuple[MetricReference, ...],
        dimension_specs: Tuple[DimensionSpec, ...],
        time_dimension_specs: Tuple[TimeDimensionSpec, ...],
        entity_specs: Tuple[EntitySpec, ...],
    ) -> List[LinkableInstanceSpec]:
        """Checks that each requested linkable instance can be retrieved for the given metric."""
        invalid_linkable_specs: List[LinkableInstanceSpec] = []
        # TODO: distinguish between dimensions that invalid via typo vs ambiguous join path
        valid_linkable_specs = self._metric_lookup.element_specs_for_metrics(metric_references=list(metric_references))

        for dimension_spec in dimension_specs:
            if dimension_spec not in valid_linkable_specs:
                invalid_linkable_specs.append(dimension_spec)

        for entity_spec in entity_specs:
            if entity_spec not in valid_linkable_specs:
                invalid_linkable_specs.append(entity_spec)

        for time_dimension_spec in time_dimension_specs:
            if (
                time_dimension_spec not in valid_linkable_specs
                # Because the metric time dimension is a virtual dimension that's not in the model, it won't be included
                # in valid_linkable_specs.
                and time_dimension_spec.reference != DataSet.metric_time_dimension_reference()
            ):
                invalid_linkable_specs.append(time_dimension_spec)

        return invalid_linkable_specs

    def _parse_order_by(
        self,
        order_by_names: Sequence[str],
        time_dimension_spec_replacements: Dict[PartialTimeDimensionSpec, TimeDimensionSpec],
    ) -> Tuple[OrderBySpec, ...]:
        """time_dimension_spec_replacements is used to replace a partial spec from parsing the names to a full one."""
        # TODO: Validate entity links
        # TODO: Validate order by items are in the query
        order_by_specs: List[OrderBySpec] = []
        for order_by_name in order_by_names:
            descending = False
            if order_by_name.startswith("-"):
                order_by_name = order_by_name[1:]
                descending = True
            parsed_name = StructuredLinkableSpecName.from_name(order_by_name)

            if MetricReference(element_name=parsed_name.element_name) in self._known_metric_names:
                if parsed_name.time_granularity:
                    raise InvalidQueryException(
                        f"Order by item '{order_by_name}' references a metric but has a time granularity"
                    )
                if parsed_name.entity_link_names:
                    raise InvalidQueryException(
                        f"Order by item '{order_by_name}' references a metric but has entity links"
                    )
                order_by_specs.append(
                    OrderBySpec(
                        metric_spec=MetricSpec(element_name=parsed_name.element_name),
                        descending=descending,
                    )
                )
            elif DimensionReference(element_name=parsed_name.element_name) in self._known_dimension_element_references:
                if parsed_name.time_granularity:
                    raise InvalidQueryException(
                        f"Order by item '{order_by_name}' references a categorical dimension but has a time "
                        f"granularity"
                    )
                order_by_specs.append(
                    OrderBySpec(
                        dimension_spec=DimensionSpec(
                            element_name=parsed_name.element_name,
                            entity_links=tuple(EntityReference(element_name=x) for x in parsed_name.entity_link_names),
                        ),
                        descending=descending,
                    )
                )
            elif (
                TimeDimensionReference(element_name=parsed_name.element_name)
                in self._known_time_dimension_element_references
            ):
                entity_links = tuple(EntityReference(element_name=x) for x in parsed_name.entity_link_names)
                if parsed_name.time_granularity:
                    order_by_specs.append(
                        OrderBySpec(
                            time_dimension_spec=TimeDimensionSpec(
                                element_name=parsed_name.element_name,
                                entity_links=entity_links,
                                time_granularity=parsed_name.time_granularity,
                            ),
                            descending=descending,
                        )
                    )
                else:
                    # If the time granularity for an order by wasn't specified, replace it with the same time
                    # granularity as it was done for the requested dimensions.
                    partial_time_dimension_spec = PartialTimeDimensionSpec(
                        element_name=parsed_name.element_name,
                        entity_links=entity_links,
                    )

                    if partial_time_dimension_spec in time_dimension_spec_replacements:
                        order_by_specs.append(
                            OrderBySpec(
                                time_dimension_spec=time_dimension_spec_replacements[partial_time_dimension_spec],
                                descending=descending,
                            )
                        )
                    else:
                        raise RequestTimeGranularityException(
                            f"Order by item '{order_by_name}' does not specify a time granularity and it does not "
                            f"match a requested time dimension"
                        )

            elif EntityReference(element_name=parsed_name.element_name) in self._known_entity_element_references:
                if parsed_name.time_granularity:
                    raise InvalidQueryException(
                        f"Order by item '{order_by_name}' references an entity but has a time granularity"
                    )
                order_by_specs.append(
                    OrderBySpec(
                        entity_spec=EntitySpec(
                            element_name=parsed_name.element_name,
                            entity_links=tuple(EntityReference(element_name=x) for x in parsed_name.entity_link_names),
                        ),
                        descending=descending,
                    )
                )
            else:
                raise InvalidQueryException(f"Order by item '{order_by_name}' references an element that is not known")

        return tuple(order_by_specs)
