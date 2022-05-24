import os
from typing import List
from yamllint import config, linter

from metricflow.model.validations.validator_helpers import ValidationContext, ValidationIssue, ValidationError


class ConfigLinter:  # noqa: D
    def __init__(self) -> None:  # noqa: D
        self._config = config.YamlLintConfig("extends: default")

    def lint_file(self, file_path: str, file_name: str) -> List[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []
        with open(file_path) as f:
            for problem in linter.run(f, self._config):
                issues.append(
                    ValidationError(
                        context=ValidationContext(file_name=file_name, line_number=problem.line),
                        message=problem.desc,  # type: ignore[misc]
                    )
                )
        return issues

    def lint_dir(self, dir_path: str) -> List[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []
        for root, _dirs, files in os.walk(dir_path):
            for file in files:
                if not (file.endswith(".yaml") or file.endswith(".yml")):
                    continue
                file_path = os.path.join(root, file)
                issues += self.lint_file(file_path, file)

        return issues
