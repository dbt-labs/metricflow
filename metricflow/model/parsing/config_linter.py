import os
from collections import OrderedDict
from typing import List, Optional
import yaml
from yamllint import config, linter, rules

from metricflow.model.validations.validator_helpers import ValidationIssue, ValidationIssueLevel

WARNING = "warning"
ERROR = "error"
DISABLE = "disable"
LEVEL = "level"


class ConfigLinter:  # noqa: D

    DEFAULT_CONFIG = {
        "yaml-files": ["*.yaml", "*.yml"],
        "rules": {
            rules.braces.ID: DISABLE,
            rules.brackets.ID: DISABLE,
            rules.colons.ID: DISABLE,
            rules.commas.ID: DISABLE,
            rules.comments.ID: DISABLE,
            rules.comments_indentation.ID: {LEVEL: ERROR},
            rules.document_start.ID: {LEVEL: WARNING},
            rules.document_end.ID: DISABLE,
            rules.empty_lines.ID: DISABLE,
            rules.empty_values.ID: {LEVEL: ERROR},
            rules.hyphens.ID: DISABLE,
            rules.indentation.ID: {LEVEL: ERROR},
            rules.key_duplicates.ID: {LEVEL: ERROR},
            rules.key_ordering.ID: DISABLE,
            rules.line_length.ID: DISABLE,
            rules.new_line_at_end_of_file.ID: DISABLE,
            rules.new_lines.ID: DISABLE,
            rules.octal_values.ID: {LEVEL: WARNING},
            rules.quoted_strings.ID: DISABLE,
            rules.trailing_spaces.ID: {LEVEL: WARNING},
            rules.truthy.ID: DISABLE,
        },
    }

    def __init__(self, lint_config: Optional[str] = None) -> None:  # noqa: D
        self._lint_config = lint_config if lint_config else yaml.dump(data=self.DEFAULT_CONFIG)
        self._config = config.YamlLintConfig(self._lint_config)

    def lint_file(self, file_path: str, file_name: str) -> List[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []
        with open(file_path) as f:
            for problem in linter.run(f, self._config):
                level = ValidationIssueLevel.ERROR if problem.level == ERROR else ValidationIssueLevel.WARNING
                issues.append(
                    ValidationIssue(
                        level=level,
                        model_object_reference=OrderedDict(),
                        message=f"In file {file_name} on line {problem.line} found issue with yaml spec:\n{problem.desc}",
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
