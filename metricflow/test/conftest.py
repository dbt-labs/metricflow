# These imports are required to properly set up pytest fixtures.
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.test.fixtures.cli_fixtures import *  # noqa: F401, F403
from metricflow.test.fixtures.dataflow_fixtures import *  # noqa: F401, F403
from metricflow.test.fixtures.id_fixtures import *  # noqa: F401, F403
from metricflow.test.fixtures.model_fixtures import *  # noqa: F401, F403
from metricflow.test.fixtures.setup_fixtures import *  # noqa: F401, F403
from metricflow.test.fixtures.sql_client_fixtures import *  # noqa: F401, F403
from metricflow.test.fixtures.sql_fixtures import *  # noqa: F401, F403
from metricflow.test.fixtures.table_fixtures import *  # noqa: F401, F403
from metricflow.time.date_part import DatePart


@dataclass
class MockQueryParameter:
    """This is a mock that is just used to test the query parser."""

    name: str
    grain: Optional[TimeGranularity] = None
    date_part: Optional[DatePart] = None
