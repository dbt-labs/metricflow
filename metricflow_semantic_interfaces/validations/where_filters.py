from __future__ import annotations

import traceback
from enum import Enum
from typing import Generic, List, Sequence, Tuple

from metricflow_semantic_interfaces.call_parameter_sets import JinjaCallParameterSets
from metricflow_semantic_interfaces.protocols import Metric, SemanticManifestT
from metricflow_semantic_interfaces.protocols.saved_query import SavedQuery
from metricflow_semantic_interfaces.references import MetricModelReference
from metricflow_semantic_interfaces.type_enums import TimeGranularity
from metricflow_semantic_interfaces.validations.validator_helpers import (
    FileContext,
    MetricContext,
    SavedQueryContext,
    SavedQueryElementType,
    SemanticManifestValidationRule,
    ValidationContext,
    ValidationIssue,
    ValidationWarning,
    generate_exception_issue,
    validate_safely,
)


class SemanticManifestNodeType(Enum):
    """Types of objects to validate (used for validation messages)."""

    SAVED_QUERY = "saved query"
    METRIC = "metric"


class WhereFiltersAreParseable(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Validates that all WhereFilters are parseable."""

    @staticmethod
    def _validate_time_granularity_names(
        element_name: str,
        object_type: SemanticManifestNodeType,
        context: ValidationContext,
        filter_call_param_sets: JinjaCallParameterSets,
        valid_granularity_names: List[str],
    ) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []

        for time_dim_call_parameter_set in filter_call_param_sets.time_dimension_call_parameter_sets:
            if not time_dim_call_parameter_set.time_granularity_name:
                continue
            if time_dim_call_parameter_set.time_granularity_name.lower() not in valid_granularity_names:
                issues.append(
                    ValidationWarning(
                        context=context,
                        message=f"Filter for {object_type} `{element_name}` is not valid. "
                        f"`{time_dim_call_parameter_set.time_granularity_name}` is not a valid granularity name. "
                        f"Valid granularity options: {valid_granularity_names}",
                    )
                )
        return issues

    @staticmethod
    def _validate_time_granularity_names_for_saved_query(
        saved_query: SavedQuery, valid_granularity_names: List[str]
    ) -> Sequence[ValidationIssue]:
        where_param = saved_query.query_params.where
        if where_param is None:
            return []

        issues: List[ValidationIssue] = []
        for where_filter in where_param.where_filters:
            issues += WhereFiltersAreParseable._validate_time_granularity_names(
                element_name=saved_query.name,
                object_type=SemanticManifestNodeType.SAVED_QUERY,
                context=SavedQueryContext(
                    file_context=FileContext.from_metadata(metadata=saved_query.metadata),
                    element_type=SavedQueryElementType.WHERE,
                    element_value=where_filter.where_sql_template,
                ),
                filter_call_param_sets=where_filter.call_parameter_sets(
                    custom_granularity_names=valid_granularity_names
                ),
                valid_granularity_names=valid_granularity_names,
            )

        return issues

    @staticmethod
    def _validate_time_granularity_names_for_metric(
        context: MetricContext,
        filter_expression_parameter_sets: Sequence[Tuple[str, JinjaCallParameterSets]],
        valid_granularity_names: List[str],
    ) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []
        for _, param_set in filter_expression_parameter_sets:
            issues += WhereFiltersAreParseable._validate_time_granularity_names(
                element_name=context.metric.metric_name,
                object_type=SemanticManifestNodeType.METRIC,
                context=context,
                filter_call_param_sets=param_set,
                valid_granularity_names=valid_granularity_names,
            )
        return issues

    @staticmethod
    @validate_safely("validating the where field in a saved query.")
    def _validate_saved_query(saved_query: SavedQuery, valid_granularity_names: List[str]) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []
        if saved_query.query_params.where is None:
            return issues
        for where_filter in saved_query.query_params.where.where_filters:
            try:
                where_filter.call_parameter_sets(custom_granularity_names=valid_granularity_names)
            except Exception as e:
                issues.append(
                    generate_exception_issue(
                        what_was_being_done=f"trying to parse a filter in saved query `{saved_query.name}`",
                        e=e,
                        context=SavedQueryContext(
                            file_context=FileContext.from_metadata(metadata=saved_query.metadata),
                            element_type=SavedQueryElementType.WHERE,
                            element_value=where_filter.where_sql_template,
                        ),
                        extras={
                            "traceback": "".join(traceback.format_tb(e.__traceback__)),
                        },
                    )
                )
            else:
                issues += WhereFiltersAreParseable._validate_time_granularity_names_for_saved_query(
                    saved_query, valid_granularity_names
                )

        return issues

    @staticmethod
    @validate_safely(
        whats_being_done="running model validation ensuring a metric's filter properties are configured properly"
    )
    def _validate_metric(metric: Metric, valid_granularity_names: List[str]) -> Sequence[ValidationIssue]:  # noqa: D102
        issues: List[ValidationIssue] = []
        context = MetricContext(
            file_context=FileContext.from_metadata(metadata=metric.metadata),
            metric=MetricModelReference(metric_name=metric.name),
        )

        if metric.filter is not None:
            try:
                metric.filter.filter_expression_parameter_sets(custom_granularity_names=valid_granularity_names)
            except Exception as e:
                issues.append(
                    generate_exception_issue(
                        what_was_being_done=f"trying to parse filter of metric `{metric.name}`",
                        e=e,
                        context=context,
                        extras={
                            "traceback": "".join(traceback.format_tb(e.__traceback__)),
                        },
                    )
                )
            else:
                issues += WhereFiltersAreParseable._validate_time_granularity_names_for_metric(
                    context=context,
                    filter_expression_parameter_sets=metric.filter.filter_expression_parameter_sets(
                        custom_granularity_names=valid_granularity_names
                    ),
                    valid_granularity_names=valid_granularity_names,
                )

        if metric.type_params:
            measure = metric.type_params.measure
            if measure is not None and measure.filter is not None:
                try:
                    measure.filter.filter_expression_parameter_sets(custom_granularity_names=valid_granularity_names)
                except Exception as e:
                    issues.append(
                        generate_exception_issue(
                            what_was_being_done=f"trying to parse filter of measure input `{measure.name}` "
                            f"on metric `{metric.name}`",
                            e=e,
                            context=context,
                            extras={
                                "traceback": "".join(traceback.format_tb(e.__traceback__)),
                            },
                        )
                    )
                else:
                    issues += WhereFiltersAreParseable._validate_time_granularity_names_for_metric(
                        context=context,
                        filter_expression_parameter_sets=measure.filter.filter_expression_parameter_sets(
                            custom_granularity_names=valid_granularity_names
                        ),
                        valid_granularity_names=valid_granularity_names,
                    )

            numerator = metric.type_params.numerator
            if numerator is not None and numerator.filter is not None:
                try:
                    numerator.filter.filter_expression_parameter_sets(custom_granularity_names=valid_granularity_names)
                except Exception as e:
                    issues.append(
                        generate_exception_issue(
                            what_was_being_done=f"trying to parse the numerator filter on metric `{metric.name}`",
                            e=e,
                            context=context,
                            extras={
                                "traceback": "".join(traceback.format_tb(e.__traceback__)),
                            },
                        )
                    )
                else:
                    issues += WhereFiltersAreParseable._validate_time_granularity_names_for_metric(
                        context=context,
                        filter_expression_parameter_sets=numerator.filter.filter_expression_parameter_sets(
                            custom_granularity_names=valid_granularity_names
                        ),
                        valid_granularity_names=valid_granularity_names,
                    )

            denominator = metric.type_params.denominator
            if denominator is not None and denominator.filter is not None:
                try:
                    denominator.filter.filter_expression_parameter_sets(
                        custom_granularity_names=valid_granularity_names
                    )
                except Exception as e:
                    issues.append(
                        generate_exception_issue(
                            what_was_being_done=f"trying to parse the denominator filter on metric `{metric.name}`",
                            e=e,
                            context=context,
                            extras={
                                "traceback": "".join(traceback.format_tb(e.__traceback__)),
                            },
                        )
                    )
                else:
                    issues += WhereFiltersAreParseable._validate_time_granularity_names_for_metric(
                        context=context,
                        filter_expression_parameter_sets=denominator.filter.filter_expression_parameter_sets(
                            custom_granularity_names=valid_granularity_names
                        ),
                        valid_granularity_names=valid_granularity_names,
                    )

            for input_metric in metric.type_params.metrics or []:
                if input_metric.filter is not None:
                    try:
                        input_metric.filter.filter_expression_parameter_sets(
                            custom_granularity_names=valid_granularity_names
                        )
                    except Exception as e:
                        issues.append(
                            generate_exception_issue(
                                what_was_being_done=f"trying to parse filter for input metric `{input_metric.name}` "
                                f"on metric `{metric.name}`",
                                e=e,
                                context=context,
                                extras={
                                    "traceback": "".join(traceback.format_tb(e.__traceback__)),
                                },
                            )
                        )
                    else:
                        issues += WhereFiltersAreParseable._validate_time_granularity_names_for_metric(
                            context=context,
                            filter_expression_parameter_sets=input_metric.filter.filter_expression_parameter_sets(
                                custom_granularity_names=valid_granularity_names
                            ),
                            valid_granularity_names=valid_granularity_names,
                        )
        return issues

    @staticmethod
    @validate_safely(whats_being_done="running manifest validation ensuring all metric where filters are parseable")
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D102
        issues: List[ValidationIssue] = []
        custom_granularity_names = [
            granularity.name
            for time_spine in semantic_manifest.project_configuration.time_spines
            for granularity in time_spine.custom_granularities
        ]
        valid_granularity_names = [
            standard_granularity.value for standard_granularity in TimeGranularity
        ] + custom_granularity_names

        for metric in semantic_manifest.metrics or []:
            issues += WhereFiltersAreParseable._validate_metric(
                metric=metric, valid_granularity_names=valid_granularity_names
            )
        for saved_query in semantic_manifest.saved_queries:
            issues += WhereFiltersAreParseable._validate_saved_query(saved_query, valid_granularity_names)

        return issues
