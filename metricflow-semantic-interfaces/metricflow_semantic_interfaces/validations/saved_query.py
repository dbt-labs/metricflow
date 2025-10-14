from __future__ import annotations

import logging
import traceback
from dataclasses import dataclass
from typing import Generic, List, Optional, Sequence, Set

from metricflow_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from metricflow_semantic_interfaces.parsing.text_input.ti_description import (
    ObjectBuilderItemDescription,
    QueryItemType,
)
from metricflow_semantic_interfaces.parsing.text_input.ti_processor import (
    ObjectBuilderTextProcessor,
)
from metricflow_semantic_interfaces.parsing.text_input.valid_method import (
    ConfiguredValidMethodMapping,
    ValidMethodMapping,
)
from metricflow_semantic_interfaces.parsing.where_filter.jinja_object_parser import (
    JinjaObjectParser,
    QueryItemLocation,
)
from metricflow_semantic_interfaces.protocols import SemanticManifestT
from metricflow_semantic_interfaces.protocols.saved_query import SavedQuery
from metricflow_semantic_interfaces.validations.validator_helpers import (
    FileContext,
    SavedQueryContext,
    SavedQueryElementType,
    SemanticManifestValidationRule,
    ValidationError,
    ValidationIssue,
    generate_exception_issue,
    validate_safely,
)

logger = logging.getLogger(__name__)


class SavedQueryRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Validates fields in a saved query.

    As the semantic model graph is not traversed completely in DSI, the validations for saved queries can't be complete.
    Consequently, the current plan is that we add a separate validation using MetricFlow in CI.

    * Check if metric names exist in the manifest.
    * Check that the where filter is valid using the same logic as WhereFiltersAreParsable
    """

    @staticmethod
    @validate_safely("Validate the group-by field in a saved query.")
    def _check_group_bys(
        valid_group_by_element_names: Set[str], saved_query: SavedQuery, custom_granularity_names: Sequence[str]
    ) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []

        for group_by_item in saved_query.query_params.group_by:
            try:
                parameter_sets = JinjaObjectParser.parse_call_parameter_sets(
                    where_sql_template="{{" + group_by_item + "}}",
                    custom_granularity_names=custom_granularity_names,
                    query_item_location=QueryItemLocation.NON_ORDER_BY,
                )
            except Exception as e:
                issues.append(
                    generate_exception_issue(
                        what_was_being_done=f"trying to parse a group-by in saved query `{saved_query.name}`",
                        e=e,
                        context=SavedQueryContext(
                            file_context=FileContext.from_metadata(metadata=saved_query.metadata),
                            element_type=SavedQueryElementType.WHERE,
                            element_value=group_by_item,
                        ),
                        extras={
                            "traceback": "".join(traceback.format_tb(e.__traceback__)),
                        },
                    )
                )
                continue

            element_names_in_group_by = (
                [x.entity_reference.element_name for x in parameter_sets.entity_call_parameter_sets]
                + [x.dimension_reference.element_name for x in parameter_sets.dimension_call_parameter_sets]
                + [x.time_dimension_reference.element_name for x in parameter_sets.time_dimension_call_parameter_sets]
                + [x.metric_reference.element_name for x in parameter_sets.metric_call_parameter_sets]
            )

            if len(element_names_in_group_by) != 1 or element_names_in_group_by[0] not in valid_group_by_element_names:
                issues.append(
                    ValidationError(
                        message=f"`{group_by_item}` is not a valid group-by name.",
                        context=SavedQueryContext(
                            file_context=FileContext.from_metadata(metadata=saved_query.metadata),
                            element_type=SavedQueryElementType.GROUP_BY,
                            element_value=group_by_item,
                        ),
                    )
                )
        return issues

    @staticmethod
    @validate_safely("Validate the metrics field in a saved query.")
    def _check_metrics(valid_metric_names: Set[str], saved_query: SavedQuery) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []
        for metric_name in saved_query.query_params.metrics:
            if metric_name not in valid_metric_names:
                issues.append(
                    ValidationError(
                        message=f"`{metric_name}` is not a valid metric name.",
                        context=SavedQueryContext(
                            file_context=FileContext.from_metadata(metadata=saved_query.metadata),
                            element_type=SavedQueryElementType.METRIC,
                            element_value=metric_name,
                        ),
                    )
                )
        return issues

    @staticmethod
    def parse_query_item(
        saved_query: SavedQuery,
        text_processor: ObjectBuilderTextProcessor,
        query_item_input: str,
        element_type: SavedQueryElementType,
        valid_method_mapping: ValidMethodMapping,
    ) -> _ParseQueryItemResult:
        """Parse a Jinja syntax object into an ObjectBuilderItemDescription."""
        try:
            item_description = text_processor.get_description(query_item_input, valid_method_mapping)
            return _ParseQueryItemResult(item_description=item_description, validation_issue=None)
        except Exception as e:
            return _ParseQueryItemResult(
                item_description=None,
                validation_issue=generate_exception_issue(
                    what_was_being_done=(
                        f"parsing a field in {saved_query.name!r}."
                        f" Note that metrics need to be specified using the object-builder syntax"
                        f" (`Metric('metric_name')`) and if `.descending(...)` is specified, it should be at the"
                        f" end."
                    ),
                    e=e,
                    context=SavedQueryContext(
                        file_context=FileContext.from_metadata(metadata=saved_query.metadata),
                        element_type=element_type,
                        element_value=query_item_input,
                    ),
                    extras={
                        "traceback": "".join(traceback.format_tb(e.__traceback__)),
                    },
                ),
            )

    @staticmethod
    @validate_safely("Validate the order-by field in a saved query.")
    def _check_order_by(saved_query: SavedQuery) -> Sequence[ValidationIssue]:
        """Check that the order-by items in a saved query are valid.

        The order-by item without the `.descending()` should match with one of the metric items or group-by items.
        """
        validation_issues: List[ValidationIssue] = []
        if len(saved_query.query_params.order_by) == 0:
            return validation_issues

        valid_query_item_descriptions = set()
        text_processor = ObjectBuilderTextProcessor()
        for metric in saved_query.query_params.metrics:
            # In an order-by, a metric is specified as "Metric('bookings')" while in the metrics section, it's only the
            # metric name.
            result = SavedQueryRule.parse_query_item(
                saved_query=saved_query,
                text_processor=text_processor,
                query_item_input=f"{QueryItemType.METRIC.value}('{metric}')",
                element_type=SavedQueryElementType.METRIC,
                valid_method_mapping=ConfiguredValidMethodMapping.DEFAULT_MAPPING,
            )
            if result.item_description is not None:
                valid_query_item_descriptions.add(result.item_description)
            if result.validation_issue is not None:
                validation_issues.append(result.validation_issue)

        for group_by in saved_query.query_params.group_by:
            result = SavedQueryRule.parse_query_item(
                saved_query=saved_query,
                text_processor=text_processor,
                query_item_input=group_by,
                element_type=SavedQueryElementType.GROUP_BY,
                valid_method_mapping=ConfiguredValidMethodMapping.DEFAULT_MAPPING,
            )
            if result.item_description is not None:
                valid_query_item_descriptions.add(result.item_description)
            if result.validation_issue is not None:
                validation_issues.append(result.validation_issue)

        # If there are issues with the metrics or group-by items, checking the order-by may lead to erroneous issues.
        if len(validation_issues) > 0:
            return validation_issues

        for order_by in saved_query.query_params.order_by:
            result = SavedQueryRule.parse_query_item(
                saved_query=saved_query,
                text_processor=text_processor,
                query_item_input=order_by,
                element_type=SavedQueryElementType.GROUP_BY,
                valid_method_mapping=ConfiguredValidMethodMapping.DEFAULT_MAPPING_FOR_ORDER_BY,
            )
            if result.validation_issue is not None:
                validation_issues.append(result.validation_issue)
                continue
            item_description = result.item_description
            assert item_description is not None, "This should have been ensured by the result class."

            # The value of `descending` should be unset as only an order-by item would have it set.
            if item_description.with_descending_unset() not in valid_query_item_descriptions:
                validation_issues.append(
                    ValidationError(
                        message=f"{order_by} does not match any of the listed metrics or group-by items. ",
                        context=SavedQueryContext(
                            file_context=FileContext.from_metadata(metadata=saved_query.metadata),
                            element_type=SavedQueryElementType.ORDER_BY,
                            element_value=order_by,
                        ),
                    )
                )

        return validation_issues

    @staticmethod
    @validate_safely("Validate the order-by field in a saved query.")
    def _check_limit(saved_query: SavedQuery) -> Sequence[ValidationIssue]:
        validation_issues: List[ValidationIssue] = []
        limit = saved_query.query_params.limit
        if limit is None:
            return validation_issues

        if limit < 0:
            validation_issues.append(
                ValidationError(
                    message=f"Invalid limit value: {limit} (should be >= 0)",
                    context=SavedQueryContext(
                        file_context=FileContext.from_metadata(metadata=saved_query.metadata),
                        element_type=SavedQueryElementType.LIMIT,
                        element_value=str(limit),
                    ),
                )
            )

        return validation_issues

    @staticmethod
    @validate_safely("Validate all saved queries in a semantic manifest.")
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D102
        issues: List[ValidationIssue] = []
        custom_granularity_names = [
            granularity.name
            for time_spine in semantic_manifest.project_configuration.time_spines
            for granularity in time_spine.custom_granularities
        ]
        valid_metric_names = {metric.name for metric in semantic_manifest.metrics}
        valid_group_by_element_names = valid_metric_names.union({METRIC_TIME_ELEMENT_NAME})
        for semantic_model in semantic_manifest.semantic_models:
            for dimension in semantic_model.dimensions:
                valid_group_by_element_names.add(dimension.name)
            for entity in semantic_model.entities:
                valid_group_by_element_names.add(entity.name)

        for saved_query in semantic_manifest.saved_queries:
            issues += SavedQueryRule._check_metrics(
                valid_metric_names=valid_metric_names,
                saved_query=saved_query,
            )
            issues += SavedQueryRule._check_group_bys(
                valid_group_by_element_names=valid_group_by_element_names,
                saved_query=saved_query,
                custom_granularity_names=custom_granularity_names,
            )
            issues += SavedQueryRule._check_order_by(saved_query)
            issues += SavedQueryRule._check_limit(saved_query)
        return issues


@dataclass(frozen=True)
class _ParseQueryItemResult:
    """Result of parsing a string like `Dimension('listing__country')`."""

    item_description: Optional[ObjectBuilderItemDescription]
    validation_issue: Optional[ValidationIssue]

    def __post_init__(self) -> None:
        assert (self.item_description is not None) ^ (self.validation_issue is not None)
