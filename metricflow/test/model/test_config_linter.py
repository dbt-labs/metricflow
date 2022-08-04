from metricflow.model.parsing.config_linter import ConfigLinter


def test_lint_dir(config_linter_model_path: str) -> None:  # noqa: D
    issues = ConfigLinter().lint_dir(dir_path=config_linter_model_path)

    assert len(issues.all_issues) == 1
    assert len(issues.errors) == 1
    assert 'duplication of key "data_source"' in issues.errors[0].as_readable_str()
