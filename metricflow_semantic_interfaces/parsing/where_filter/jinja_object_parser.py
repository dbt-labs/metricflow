from __future__ import annotations

from typing import Sequence

from metricflow_semantic_interfaces.call_parameter_sets import (
    JinjaCallParameterSets,
    ParseJinjaObjectException,
)
from metricflow_semantic_interfaces.enum_extension import assert_values_exhausted
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
from metricflow_semantic_interfaces.parsing.where_filter.parameter_set_factory import (
    ParameterSetFactory,
    QueryItemLocation,
)


class JinjaObjectParser:
    """Parses the template in the Jinja object-builder syntax into JinjaCallParameterSets.

    These are used in where filters, saved query params, and the JDBC API.
    """

    @staticmethod
    def parse_item_descriptions(
        where_sql_template: str,
        valid_method_mapping: ValidMethodMapping = ConfiguredValidMethodMapping.DEFAULT_MAPPING,
    ) -> Sequence[ObjectBuilderItemDescription]:
        """Parses the filter and returns the item descriptions."""
        text_processor = ObjectBuilderTextProcessor()

        try:
            return text_processor.collect_descriptions_from_template(
                jinja_template=where_sql_template, valid_method_mapping=valid_method_mapping
            )
        except Exception as e:
            raise ParseJinjaObjectException(f"Error while parsing Jinja template:\n{where_sql_template}") from e

    @staticmethod
    def parse_call_parameter_sets(
        where_sql_template: str,
        custom_granularity_names: Sequence[str],
        query_item_location: QueryItemLocation,
    ) -> JinjaCallParameterSets:
        """Return the result of extracting the semantic objects referenced in the where SQL template string."""
        valid_method_mapping = (
            ConfiguredValidMethodMapping.DEFAULT_MAPPING_FOR_ORDER_BY
            if query_item_location == QueryItemLocation.ORDER_BY
            else ConfiguredValidMethodMapping.DEFAULT_MAPPING
        )
        descriptions = JinjaObjectParser.parse_item_descriptions(
            where_sql_template, valid_method_mapping=valid_method_mapping
        )

        """
        Dimensions that are created with a grain or date_part parameter, for instance Dimension(...).grain(...), are
        added to time_dimension_call_parameter_sets otherwise they are add to dimension_call_parameter_sets
        """
        dimension_call_parameter_sets = []
        time_dimension_call_parameter_sets = []
        entity_call_parameter_sets = []
        metric_call_parameter_sets = []

        for description in descriptions:
            item_type = description.item_type

            if item_type is QueryItemType.DIMENSION:
                if description.time_granularity_name or description.date_part_name:
                    time_dimension_call_parameter_sets.append(
                        ParameterSetFactory.create_time_dimension(
                            time_dimension_name=description.item_name,
                            time_granularity_name=description.time_granularity_name,
                            entity_path=description.entity_path,
                            date_part_name=description.date_part_name,
                            custom_granularity_names=custom_granularity_names,
                            descending=description.descending,
                        )
                    )
                else:
                    dimension_call_parameter_sets.append(
                        ParameterSetFactory.create_dimension(
                            dimension_name=description.item_name,
                            entity_path=description.entity_path,
                            descending=description.descending,
                        )
                    )
            elif item_type is QueryItemType.TIME_DIMENSION:
                time_dimension_call_parameter_sets.append(
                    ParameterSetFactory.create_time_dimension(
                        time_dimension_name=description.item_name,
                        time_granularity_name=description.time_granularity_name,
                        entity_path=description.entity_path,
                        date_part_name=description.date_part_name,
                        custom_granularity_names=custom_granularity_names,
                        descending=description.descending,
                    )
                )
            elif item_type is QueryItemType.ENTITY:
                entity_call_parameter_sets.append(
                    ParameterSetFactory.create_entity(
                        entity_name=description.item_name,
                        entity_path=description.entity_path,
                        descending=description.descending,
                    )
                )
            elif item_type is QueryItemType.METRIC:
                metric_call_parameter_sets.append(
                    ParameterSetFactory.create_metric(
                        metric_name=description.item_name,
                        group_by=description.group_by_for_metric_item,
                        query_item_location=query_item_location,
                        descending=description.descending,
                    )
                )
            else:
                assert_values_exhausted(item_type)

        return JinjaCallParameterSets(
            dimension_call_parameter_sets=tuple(dimension_call_parameter_sets),
            time_dimension_call_parameter_sets=tuple(time_dimension_call_parameter_sets),
            entity_call_parameter_sets=tuple(entity_call_parameter_sets),
            metric_call_parameter_sets=tuple(metric_call_parameter_sets),
        )
