from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

from dbt_semantic_interfaces.protocols.dimension import Dimension, DimensionType
from dbt_semantic_interfaces.protocols.entity import Entity
from dbt_semantic_interfaces.protocols.measure import Measure
from dbt_semantic_interfaces.protocols.semantic_model import SemanticModel
from dbt_semantic_interfaces.references import SemanticModelElementReference, SemanticModelReference
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.aggregation_properties import AggregationState
from metricflow.dag.id_generation import IdGeneratorRegistry
from metricflow.dataflow.sql_table import SqlTable
from metricflow.dataset.semantic_model_adapter import SemanticModelDataSet
from metricflow.instances import (
    DimensionInstance,
    EntityInstance,
    InstanceSet,
    MeasureInstance,
    TimeDimensionInstance,
)
from metricflow.model.semantics.semantic_model_lookup import SemanticModelLookup
from metricflow.model.spec_converters import MeasureConverter
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.specs import (
    DEFAULT_TIME_GRANULARITY,
    DimensionSpec,
    EntityReference,
    EntitySpec,
    TimeDimensionSpec,
)
from metricflow.sql.sql_exprs import (
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlDateTruncExpression,
    SqlExpressionNode,
    SqlExtractExpression,
    SqlStringExpression,
)
from metricflow.sql.sql_plan import (
    SqlQueryPlanNode,
    SqlSelectColumn,
    SqlSelectStatementNode,
    SqlTableFromClauseNode,
)
from metricflow.time.date_part import DatePart

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DimensionConversionResult:
    """Helper class for returning the results of converting dimensions from a semantic model."""

    dimension_instances: Sequence[DimensionInstance]
    time_dimension_instances: Sequence[TimeDimensionInstance]
    select_columns: Sequence[SqlSelectColumn]


class SemanticModelToDataSetConverter:
    """Converts a semantic model in the model to a data set that can be used with the dataflow plan builder.

    Entity links generally refer to the entities used to join the measure source to the dimension source. For
    example, the dimension name "user_id__device_id__platform" has entity links "user_id" and "device_id" and would
    mean that the measure source was joined by "user_id" to an intermediate source, and then it was joined by
    "device_id" to the source containing the "platform" dimension.
    """

    # Regex for inferring whether an expression for an element is a column reference.
    _SQL_IDENTIFIER_REGEX = re.compile("^[a-zA-Z_][a-zA-Z_0-9]*$")

    def __init__(self, column_association_resolver: ColumnAssociationResolver) -> None:  # noqa: D
        self._column_association_resolver = column_association_resolver

    def _create_dimension_instance(
        self,
        semantic_model_name: str,
        dimension: Dimension,
        entity_links: Tuple[EntityReference, ...],
    ) -> DimensionInstance:
        """Create a dimension instance from the dimension object in the model."""
        dimension_spec = DimensionSpec(
            element_name=dimension.reference.element_name,
            entity_links=entity_links,
        )
        return DimensionInstance(
            associated_columns=(self._column_association_resolver.resolve_spec(dimension_spec),),
            spec=dimension_spec,
            defined_from=(
                SemanticModelElementReference(
                    semantic_model_name=semantic_model_name,
                    element_name=dimension.reference.element_name,
                ),
            ),
        )

    def _create_time_dimension_instance(
        self,
        semantic_model_name: str,
        time_dimension: Dimension,
        entity_links: Tuple[EntityReference, ...],
        time_granularity: TimeGranularity = DEFAULT_TIME_GRANULARITY,
        date_part: Optional[DatePart] = None,
    ) -> TimeDimensionInstance:
        """Create a time dimension instance from the dimension object from a semantic model in the model."""
        time_dimension_spec = TimeDimensionSpec(
            element_name=time_dimension.reference.element_name,
            entity_links=entity_links,
            time_granularity=time_granularity,
            date_part=date_part,
        )

        return TimeDimensionInstance(
            associated_columns=(self._column_association_resolver.resolve_spec(time_dimension_spec),),
            spec=time_dimension_spec,
            defined_from=(
                SemanticModelElementReference(
                    semantic_model_name=semantic_model_name,
                    element_name=time_dimension.reference.element_name,
                ),
            ),
        )

    def _create_entity_instance(
        self,
        semantic_model_name: str,
        entity: Entity,
        entity_links: Tuple[EntityReference, ...],
    ) -> EntityInstance:
        """Create an entity instance from the entity object from a semantic modelin the model."""
        entity_spec = EntitySpec(
            element_name=entity.reference.element_name,
            entity_links=entity_links,
        )
        return EntityInstance(
            associated_columns=(self._column_association_resolver.resolve_spec(entity_spec),),
            spec=entity_spec,
            defined_from=(
                SemanticModelElementReference(
                    semantic_model_name=semantic_model_name,
                    element_name=entity.reference.element_name,
                ),
            ),
        )

    @staticmethod
    def _make_element_sql_expr(
        table_alias: str, element_name: str, element_expr: Optional[str] = None
    ) -> SqlExpressionNode:
        """Create an expression that can be used for reading the element from the semantic model's SQL."""
        if element_expr:
            if SemanticModelToDataSetConverter._SQL_IDENTIFIER_REGEX.match(
                element_expr
            ) and element_expr.upper() not in (
                "TRUE",
                "FALSE",
                "NULL",
            ):
                return SqlColumnReferenceExpression(
                    SqlColumnReference(
                        table_alias=table_alias,
                        column_name=element_expr,
                    )
                )
            return SqlStringExpression(sql_expr=element_expr)

        return SqlColumnReferenceExpression(
            SqlColumnReference(
                table_alias=table_alias,
                column_name=element_name,
            )
        )

    def _convert_measures(
        self,
        semantic_model_name: str,
        measures: Sequence[Measure],
        table_alias: str,
    ) -> Tuple[Sequence[MeasureInstance], Sequence[SqlSelectColumn]]:
        # Convert all elements to instances
        measure_instances = []
        select_columns = []
        for measure in measures or []:
            measure_spec = MeasureConverter.convert_to_measure_spec(measure=measure)
            measure_instance = MeasureInstance(
                associated_columns=(self._column_association_resolver.resolve_spec(measure_spec),),
                spec=measure_spec,
                defined_from=(
                    SemanticModelElementReference(
                        semantic_model_name=semantic_model_name,
                        element_name=measure.reference.element_name,
                    ),
                ),
                aggregation_state=AggregationState.NON_AGGREGATED,
            )
            measure_instances.append(measure_instance)
            select_columns.append(
                SqlSelectColumn(
                    expr=SemanticModelToDataSetConverter._make_element_sql_expr(
                        table_alias=table_alias,
                        element_name=measure.reference.element_name,
                        element_expr=measure.expr,
                    ),
                    column_alias=measure_instance.associated_column.column_name,
                )
            )

        return measure_instances, select_columns

    def _convert_dimensions(
        self,
        semantic_model_name: str,
        dimensions: Sequence[Dimension],
        entity_links: Tuple[EntityReference, ...],
        table_alias: str,
    ) -> DimensionConversionResult:
        dimension_instances = []
        time_dimension_instances = []
        select_columns = []

        for dimension in dimensions or []:
            dimension_select_expr = SemanticModelToDataSetConverter._make_element_sql_expr(
                table_alias=table_alias,
                element_name=dimension.reference.element_name,
                element_expr=dimension.expr,
            )
            if dimension.type == DimensionType.CATEGORICAL:
                dimension_instance = self._create_dimension_instance(
                    semantic_model_name=semantic_model_name,
                    dimension=dimension,
                    entity_links=entity_links,
                )
                dimension_instances.append(dimension_instance)
                select_columns.append(
                    SqlSelectColumn(
                        expr=dimension_select_expr,
                        column_alias=dimension_instance.associated_column.column_name,
                    )
                )
            elif dimension.type == DimensionType.TIME:
                derived_time_dimension_instances, time_select_columns = self._convert_time_dimension(
                    dimension_select_expr=dimension_select_expr,
                    dimension=dimension,
                    semantic_model_name=semantic_model_name,
                    entity_links=entity_links,
                )
                time_dimension_instances += derived_time_dimension_instances
                select_columns += time_select_columns
            else:
                assert False, f"Unhandled dimension type: {dimension.type}"

        return DimensionConversionResult(
            dimension_instances=dimension_instances,
            time_dimension_instances=time_dimension_instances,
            select_columns=select_columns,
        )

    def _convert_time_dimension(
        self,
        dimension_select_expr: SqlExpressionNode,
        dimension: Dimension,
        semantic_model_name: str,
        entity_links: Tuple[EntityReference, ...],
    ) -> Tuple[List[TimeDimensionInstance], List[SqlSelectColumn]]:
        """Converts Dimension objects with type TIME into the relevant DataSet columns.

        Time dimensions require special handling that includes adding additional instances
        and select column statements for each granularity and date part
        """
        time_dimension_instances: List[TimeDimensionInstance] = []
        select_columns: List[SqlSelectColumn] = []

        defined_time_granularity = TimeGranularity.DAY
        if dimension.type_params and dimension.type_params.time_granularity:
            defined_time_granularity = dimension.type_params.time_granularity

        time_dimension_instance = self._create_time_dimension_instance(
            semantic_model_name=semantic_model_name,
            time_dimension=dimension,
            entity_links=entity_links,
            time_granularity=defined_time_granularity,
        )
        time_dimension_instances.append(time_dimension_instance)

        # Until we support minimal granularities, we cannot truncate for
        # any time dimension used as part of a validity window, since a validity window might
        # be stored in seconds while we would truncate to daily.
        if dimension.validity_params:
            select_columns.append(
                SqlSelectColumn(
                    expr=dimension_select_expr,
                    column_alias=time_dimension_instance.associated_column.column_name,
                )
            )
        else:
            select_columns.append(
                SqlSelectColumn(
                    expr=SqlDateTruncExpression(time_granularity=defined_time_granularity, arg=dimension_select_expr),
                    column_alias=time_dimension_instance.associated_column.column_name,
                )
            )

        # Add time dimensions with a smaller granularity for ease in query resolution
        for time_granularity in TimeGranularity:
            if time_granularity.to_int() > defined_time_granularity.to_int():
                time_dimension_instance = self._create_time_dimension_instance(
                    semantic_model_name=semantic_model_name,
                    time_dimension=dimension,
                    entity_links=entity_links,
                    time_granularity=time_granularity,
                )
                time_dimension_instances.append(time_dimension_instance)

                select_columns.append(
                    SqlSelectColumn(
                        expr=SqlDateTruncExpression(time_granularity=time_granularity, arg=dimension_select_expr),
                        column_alias=time_dimension_instance.associated_column.column_name,
                    )
                )

        # Add all date part options for easy query resolution
        for date_part in DatePart:
            if date_part.to_int() >= defined_time_granularity.to_int():
                time_dimension_instance = self._create_time_dimension_instance(
                    semantic_model_name=semantic_model_name,
                    time_dimension=dimension,
                    entity_links=entity_links,
                    time_granularity=defined_time_granularity,
                    date_part=date_part,
                )
                time_dimension_instances.append(time_dimension_instance)

                select_columns.append(
                    SqlSelectColumn(
                        expr=SqlExtractExpression(date_part=date_part, arg=dimension_select_expr),
                        column_alias=time_dimension_instance.associated_column.column_name,
                    )
                )

        return (time_dimension_instances, select_columns)

    def _create_entity_instances(
        self,
        semantic_model_name: str,
        entities: Sequence[Entity],
        entity_links: Tuple[EntityReference, ...],
        table_alias: str,
    ) -> Tuple[Sequence[EntityInstance], Sequence[SqlSelectColumn]]:
        entity_instances = []
        select_columns = []
        for entity in entities or []:
            # We don't want to create something like user_id__user_id, so skip if the link is the same as the
            # entity.
            if len(entity_links) == 1 and entity.reference == entity_links[0]:
                continue

            entity_instance = self._create_entity_instance(
                semantic_model_name=semantic_model_name,
                entity=entity,
                entity_links=entity_links,
            )

            entity_instances.append(entity_instance)
            select_columns.append(
                SqlSelectColumn(
                    expr=SemanticModelToDataSetConverter._make_element_sql_expr(
                        table_alias=table_alias,
                        element_name=entity.reference.element_name,
                        element_expr=entity.expr,
                    ),
                    column_alias=entity_instance.associated_column.column_name,
                )
            )
        return entity_instances, select_columns

    def create_sql_source_data_set(self, semantic_model: SemanticModel) -> SemanticModelDataSet:
        """Create an SQL source data set from a semantic model in the model."""
        # Gather all instances and columns from all semantic models.
        all_measure_instances: List[MeasureInstance] = []
        all_dimension_instances: List[DimensionInstance] = []
        all_time_dimension_instances: List[TimeDimensionInstance] = []
        all_entity_instances: List[EntityInstance] = []

        all_select_columns: List[SqlSelectColumn] = []
        from_source_alias = IdGeneratorRegistry.for_class(self.__class__).create_id(f"{semantic_model.name}_src")

        # Handle measures
        if len(semantic_model.measures) > 0:
            measure_instances, select_columns = self._convert_measures(
                semantic_model_name=semantic_model.name,
                measures=semantic_model.measures,
                table_alias=from_source_alias,
            )
            all_measure_instances.extend(measure_instances)
            all_select_columns.extend(select_columns)

        # Group by items in the semantic model can be accessed though a subset of the entities defined in the model.
        possible_entity_links: List[Tuple[EntityReference, ...]] = [
            (),
        ]

        for entity_link in SemanticModelLookup.entity_links_for_local_elements(semantic_model):
            possible_entity_links.append((entity_link,))

        # Handle dimensions
        conversion_results = [
            self._convert_dimensions(
                semantic_model_name=semantic_model.name,
                dimensions=semantic_model.dimensions,
                entity_links=entity_links,
                table_alias=from_source_alias,
            )
            for entity_links in possible_entity_links
        ]

        all_dimension_instances.extend(
            [
                dimension_instance
                for conversion_result in conversion_results
                for dimension_instance in conversion_result.dimension_instances
            ]
        )

        all_time_dimension_instances.extend(
            [
                time_dimension_instance
                for conversion_result in conversion_results
                for time_dimension_instance in conversion_result.time_dimension_instances
            ]
        )

        all_select_columns.extend(
            [
                select_column
                for conversion_result in conversion_results
                for select_column in conversion_result.select_columns
            ]
        )

        # Handle entities
        for entity_links in possible_entity_links:
            entity_instances, select_columns = self._create_entity_instances(
                semantic_model_name=semantic_model.name,
                entities=semantic_model.entities,
                entity_links=entity_links,
                table_alias=from_source_alias,
            )
            all_entity_instances.extend(entity_instances)
            all_select_columns.extend(select_columns)

        # Generate the "from" clause depending on whether it's an SQL query or an SQL table.
        from_source: Optional[SqlQueryPlanNode] = None
        from_source = SqlTableFromClauseNode(sql_table=SqlTable.from_string(semantic_model.node_relation.relation_name))

        select_statement_node = SqlSelectStatementNode(
            description=f"Read Elements From Semantic Model '{semantic_model.name}'",
            select_columns=tuple(all_select_columns),
            from_source=from_source,
            from_source_alias=from_source_alias,
            joins_descs=(),
            where=None,
            group_bys=(),
            order_bys=(),
        )

        return SemanticModelDataSet(
            semantic_model_reference=SemanticModelReference(semantic_model_name=semantic_model.name),
            instance_set=InstanceSet(
                measure_instances=tuple(all_measure_instances),
                dimension_instances=tuple(all_dimension_instances),
                time_dimension_instances=tuple(all_time_dimension_instances),
                entity_instances=tuple(all_entity_instances),
                metric_instances=(),
            ),
            sql_select_node=select_statement_node,
        )
