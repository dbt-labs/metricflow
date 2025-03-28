from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Sequence, Tuple

import _pytest.mark
import pytest

from tests_metricflow.cli.isolated_cli_command_interface import IsolatedCliCommandEnum


class CliProjectEnum(Enum):
    """Enumerates the different dbt projects used in CLI tests."""

    TUTORIAL = "tutorial"
    DEMO_DATA_TYPES = "demo_data_types"


@dataclass(frozen=True)
class CliTestCase:
    """Captures parameters for testing the CLI."""

    test_name: str
    description: str
    project_enum: CliProjectEnum
    expectation_description: Optional[str]
    command_enum: IsolatedCliCommandEnum
    arguments: Tuple[str, ...]
    expected_exit_code: int = 0


def cli_test_cases() -> Sequence[Tuple[_pytest.mark.ParameterSet]]:
    """Return all CLI test cases."""
    return tuple(
        (pytest.param(case, id=case.test_name),)
        for case in (
            CliTestCase(
                test_name="query",
                description="Test a minimal MF query.",
                project_enum=CliProjectEnum.TUTORIAL,
                expectation_description="A table showing the `transactions` metric",
                command_enum=IsolatedCliCommandEnum.MF_QUERY,
                arguments=_build_arguments_for_metrics_query(
                    metric_names=["transactions"], group_by_names=["metric_time"]
                ),
            ),
        )
    )


def _build_arguments_for_metrics_query(metric_names: Sequence[str], group_by_names: Sequence[str]) -> Tuple[str, ...]:
    arguments: List[str] = [
        "--metrics",
        ",".join(metric_names),
        "--group-by",
        ",".join(group_by_names),
        "--order",
        # Could be ordered by metric names as well if necessary.
        ",".join(tuple(group_by_names)),
    ]
    return tuple(arguments)


# @pytest.mark.parametrize(
#     "foo",
#     (1,),
# )
# def test_cli_with_tutorial_project(
#     foo,
#     request: FixtureRequest,
#     mf_test_configuration: MetricFlowTestConfiguration,
# ) -> None:
#     cli_test_case = CliTestCase(
#         test_name="query",
#         description="Test a minimal MF query.",
#         project_enum=CliProjectEnum.TUTORIAL,
#         expectation_description="A table showing the `transactions` metric",
#         command_enum=IsolatedCliCommandEnum.MF_QUERY,
#         arguments=_build_arguments_for_metrics_query(metric_names=["transactions"], group_by_names=["metric_time"]),
#     )
#     logger.info(LazyFormat("Got case", test_name=cli_test_case.test_name))
