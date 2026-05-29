from __future__ import annotations

import logging

import click

from metricflow_semantic_interfaces.validations.validator_helpers import (
    SemanticManifestValidationResults,
    ValidationIssue,
    ValidationIssueLevel,
)

logger = logging.getLogger(__name__)


ISSUE_COLOR_MAP = {
    ValidationIssueLevel.WARNING: "cyan",
    ValidationIssueLevel.ERROR: "bright_red",
    ValidationIssueLevel.FUTURE_ERROR: "bright_yellow",
}


class ValidationResultFormatter:
    """Helps format the results of validating a semantic manifest."""

    @staticmethod
    def format_validation_issue(issue: ValidationIssue, verbose: bool = False) -> str:
        """Returns a color-coded readable string for rendering issues in the CLI."""
        return issue.as_readable_str(
            verbose=verbose, prefix=click.style(issue.level.name, bold=True, fg=ISSUE_COLOR_MAP[issue.level])
        )

    @staticmethod
    def format_summary(result: SemanticManifestValidationResults) -> str:
        """Returns a stylized summary string for issues."""
        errors = click.style(
            text=f"{ValidationIssueLevel.ERROR.name_plural}: {len(result.errors)}",
            fg=ISSUE_COLOR_MAP[ValidationIssueLevel.ERROR],
        )
        future_errors = click.style(
            text=f"{ValidationIssueLevel.FUTURE_ERROR.name_plural}: {len(result.future_errors)}",
            fg=ISSUE_COLOR_MAP[ValidationIssueLevel.FUTURE_ERROR],
        )
        warnings = click.style(
            text=f"{ValidationIssueLevel.WARNING.name_plural}: {len(result.warnings)}",
            fg=ISSUE_COLOR_MAP[ValidationIssueLevel.WARNING],
        )
        return f"{errors}, {future_errors}, {warnings}"
