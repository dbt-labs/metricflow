from __future__ import annotations

from typing import Optional

import click
from packaging.version import Version


class CliLink:
    """Groups methods for links / URLs that displayed through the CLI."""

    TIME_SPINE_DOCS = "https://docs.getdbt.com/docs/build/metricflow-time-spine"
    REPORT_ISSUE = "https://github.com/dbt-labs/metricflow/issues"
    REPOSITORY = "https://github.com/dbt-labs/metricflow"

    @staticmethod
    def _blue_text(url: str) -> str:
        return click.style(url, fg="blue", bold=True)

    @staticmethod
    def get_time_spine_docs_link(dbt_core_version: Optional[Version]) -> str:  # noqa: D102
        time_spine_docs_url = CliLink.TIME_SPINE_DOCS
        if dbt_core_version is not None:
            time_spine_docs_url = time_spine_docs_url + f"?version={dbt_core_version.major}.{dbt_core_version.minor}"
        return CliLink._blue_text(time_spine_docs_url)

    @staticmethod
    def get_report_issue_link() -> str:  # noqa: D102
        return CliLink._blue_text(CliLink.REPORT_ISSUE)

    @staticmethod
    def get_github_repository_link() -> str:  # noqa: D102
        return CliLink._blue_text(CliLink.REPOSITORY)
