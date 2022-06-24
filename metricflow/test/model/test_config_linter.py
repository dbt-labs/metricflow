from metricflow.model.parsing.config_linter import ConfigLinter
from metricflow.model.validations.validator_helpers import ValidationIssueLevel


def test_lint_dir(config_linter_model_path: str) -> None:  # noqa: D
    issues = ConfigLinter().lint_dir(dir_path=config_linter_model_path)

    assert len(issues) == 1
    assert issues[0].level == ValidationIssueLevel.ERROR
    assert 'duplication of key "data_source"' in issues[0].as_readable_str()
