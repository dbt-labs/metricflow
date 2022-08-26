"""Helper classes to convert model objects to spec object representations

In some cases we need to take a model object and convert it to a spec for use in a DataflowPlan or similar.
This fundamentally requires us to combine a model object with a spec object and, in some cases, follow semantic
metadata about linked specs in order to resolve dimension names and the like.

Since model objects and specs should not depend on each other, we do these conversions separately via external
classes, rather than the perhaps more natural approach of adding a to_spec() method on the model objects. These
shims likely point to the need for a bit of an internal refactor, but that's a concern for another time.
"""

from typing import List

from metricflow.dataset.dataset import DataSet
from metricflow.model.objects.constraints.where import WhereClauseConstraint
from metricflow.model.objects.elements.dimension import DimensionType
from metricflow.model.objects.elements.measure import Measure
from metricflow.protocols.semantics import DataSourceSemanticsAccessor
from metricflow.query.query_exceptions import InvalidQueryException
from metricflow.specs import (
    DimensionSpec,
    IdentifierSpec,
    LinkableSpecSet,
    MeasureSpec,
    NonAdditiveDimensionSpec,
    SpecWhereClauseConstraint,
    StructuredLinkableSpecName,
    TimeDimensionSpec,
)


class MeasureConverter:
    """Static class for converting Measure model objects to MeasureSpec instances"""

    @staticmethod
    def convert_to_measure_spec(measure: Measure) -> MeasureSpec:
        """Converts a Measure to a MeasureSpec, and properly handles non-additive dimension properties"""
        non_additive_dimension_spec = (
            NonAdditiveDimensionSpec(
                name=measure.non_additive_dimension.name,
                window_choice=measure.non_additive_dimension.window_choice,
                window_groupings=tuple(measure.non_additive_dimension.window_groupings),
            )
            if measure.non_additive_dimension is not None
            else None
        )

        return MeasureSpec(
            element_name=measure.name,
            non_additive_dimension_spec=non_additive_dimension_spec,
        )


class WhereConstraintConverter:
    """Static class for converting WhereClauseConstraint objects to a SpecWhereClauseConstraint representation.

    The WhereClauseConstraint model object contains a parsed set of element names, and as such conversion to
    a SpecWhereClauseConstraint requires semantic resolution of linkable specs across data sources. This resolution
    has to happen for metrics and measures independently, as both can be constrained, so bolting this on
    to something like DataSourceSemantics will not be adequate.
    """

    @staticmethod
    def _convert_to_linkable_specs(
        data_source_semantics: DataSourceSemanticsAccessor, where_constraint_names: List[str]
    ) -> LinkableSpecSet:
        """Processes where_clause_constraint.linkable_names into associated LinkableInstanceSpecs (dims, times, ids)

        where_constraint_names: WhereConstraintClause.linkable_names
        data_source_semantics: DataSourceSemanticsAccessor from the instantiated class

        output: InstanceSpecSet of Tuple(DimensionSpec), Tuple(TimeDimensionSpec), Tuple(IdentifierSpec)
        """
        where_constraint_dimensions = []
        where_constraint_time_dimensions = []
        where_constraint_identifiers = []
        linkable_spec_names = [
            StructuredLinkableSpecName.from_name(linkable_name) for linkable_name in where_constraint_names
        ]
        dimension_references = {
            dimension_reference.element_name: dimension_reference
            for dimension_reference in data_source_semantics.get_dimension_references()
        }
        identifier_references = {
            identifier_reference.element_name: identifier_reference
            for identifier_reference in data_source_semantics.get_identifier_references()
        }

        for spec_name in linkable_spec_names:
            if spec_name.element_name == DataSet.metric_time_dimension_name():
                where_constraint_time_dimensions.append(TimeDimensionSpec.from_name(spec_name.qualified_name))
            elif spec_name.element_name in dimension_references:
                dimension = data_source_semantics.get_dimension(dimension_references[spec_name.element_name])
                if dimension.type == DimensionType.CATEGORICAL:
                    where_constraint_dimensions.append(DimensionSpec.from_name(spec_name.qualified_name))
                elif dimension.type == DimensionType.TIME:
                    where_constraint_time_dimensions.append(TimeDimensionSpec.from_name(spec_name.qualified_name))
                else:
                    raise RuntimeError(f"Unhandled type: {dimension.type}")
            elif spec_name.element_name in identifier_references:
                where_constraint_identifiers.append(IdentifierSpec.from_name(spec_name.qualified_name))
            else:
                raise InvalidQueryException(f"Unknown element: {spec_name}")

        return LinkableSpecSet(
            dimension_specs=tuple(where_constraint_dimensions),
            time_dimension_specs=tuple(where_constraint_time_dimensions),
            identifier_specs=tuple(where_constraint_identifiers),
        )

    @staticmethod
    def convert_to_spec_where_constraint(
        data_source_semantics: DataSourceSemanticsAccessor, where_constraint: WhereClauseConstraint
    ) -> SpecWhereClauseConstraint:
        """Converts a where constraint to one using specs."""
        return SpecWhereClauseConstraint(
            where_condition=where_constraint.where,
            linkable_names=tuple(where_constraint.linkable_names),
            linkable_spec_set=WhereConstraintConverter._convert_to_linkable_specs(
                data_source_semantics=data_source_semantics,
                where_constraint_names=where_constraint.linkable_names,
            ),
            execution_parameters=where_constraint.sql_params,
        )
