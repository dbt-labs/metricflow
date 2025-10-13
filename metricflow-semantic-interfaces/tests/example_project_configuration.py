from __future__ import annotations

import textwrap

from metricflow_semantic_interfaces.implementations.node_relation import PydanticNodeRelation
from metricflow_semantic_interfaces.implementations.project_configuration import (
    PydanticProjectConfiguration,
)
from metricflow_semantic_interfaces.implementations.time_spine import (
    PydanticTimeSpine,
    PydanticTimeSpineCustomGranularityColumn,
    PydanticTimeSpinePrimaryColumn,
)
from metricflow_semantic_interfaces.implementations.time_spine_table_configuration import (
    PydanticTimeSpineTableConfiguration,
)
from metricflow_semantic_interfaces.parsing.objects import YamlConfigFile
from metricflow_semantic_interfaces.type_enums import TimeGranularity

EXAMPLE_PROJECT_CONFIGURATION = PydanticProjectConfiguration(
    time_spine_table_configurations=[
        PydanticTimeSpineTableConfiguration(
            location="example_schema.example_table",
            column_name="ds",
            grain=TimeGranularity.DAY,
        )
    ],
    time_spines=[
        PydanticTimeSpine(
            node_relation=PydanticNodeRelation(alias="day_time_spine", schema_name="stuff"),
            primary_column=PydanticTimeSpinePrimaryColumn(name="ds_day", time_granularity=TimeGranularity.DAY),
            custom_granularities=[
                PydanticTimeSpineCustomGranularityColumn(name="retail_year"),
                PydanticTimeSpineCustomGranularityColumn(name="martian_week", column_name="meep_meep_wk"),
            ],
        )
    ],
)

EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE = YamlConfigFile(
    filepath="projection_configuration_yaml_file_path",
    contents=textwrap.dedent(
        """\
        project_configuration:
          time_spine_table_configurations:
            - location: example_schema.example_table
              column_name: ds
              grain: day
          time_spines:
            - node_relation:
                schema_name: stuff
                alias: day_time_spine
              primary_column:
                name: ds_day
                time_granularity: day
            - node_relation:
                schema_name: stuffs
                alias: week_time_spine
              primary_column:
                name: ds
                time_granularity: week
              custom_granularities:
                - name: martian_week
        """
    ),
)
