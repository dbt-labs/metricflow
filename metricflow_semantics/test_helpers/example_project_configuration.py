from __future__ import annotations

import textwrap

from dbt_semantic_interfaces.implementations.project_configuration import (
    PydanticProjectConfiguration,
)
from dbt_semantic_interfaces.implementations.time_spine_table_configuration import (
    PydanticTimeSpineTableConfiguration,
)
from dbt_semantic_interfaces.parsing.objects import YamlConfigFile
from dbt_semantic_interfaces.type_enums import TimeGranularity

EXAMPLE_PROJECT_CONFIGURATION = PydanticProjectConfiguration(
    time_spine_table_configurations=[
        PydanticTimeSpineTableConfiguration(
            location="example_schema.example_table",
            column_name="ds",
            grain=TimeGranularity.DAY,
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
        """
    ),
)
