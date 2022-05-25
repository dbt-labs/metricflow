import os
from collections import OrderedDict
from typing import List, Optional
import yaml
from yamllint import config, linter

from metricflow.model.validations.validator_helpers import ValidationIssue, ValidationError

WARNING = "warning"
ERROR = "error"
DISABLE = "disable"
LEVEL = "level"


class ConfigLinter:  # noqa: D

    DEFAULT_CONFIG = {
        "yaml-files": ["*.yaml", "*.yml"],
        "rules": {
            "braces": DISABLE,
            "brackets": DISABLE,
            "colons": DISABLE,
            "commas": DISABLE,
            "comments": DISABLE,
            "comments-indentation": {LEVEL: ERROR},
            "document-start": {LEVEL: WARNING},
            "document-end": DISABLE,
            "empty-lines": DISABLE,
            "empty-values": {LEVEL: ERROR},
            "hyphens": DISABLE,
            "indentation": {LEVEL: ERROR},
            "key-duplicates": {LEVEL: ERROR},
            "key-ordering": DISABLE,
            "line-length": DISABLE,
            "new-line-at-end-of-file": DISABLE,
            "new-lines": DISABLE,
            "octal-values": {LEVEL: WARNING},
            "quoted-strings": DISABLE,
            "trailing-spaces": {LEVEL: WARNING},
            "truthy": DISABLE,
        },
    }

    def __init__(self, lint_config: Optional[str] = None) -> None:  # noqa: D
        self._lint_config = lint_config if lint_config else yaml.dump(data=self.DEFAULT_CONFIG)
        self._config = config.YamlLintConfig(self._lint_config)

    def lint_file(self, file_path: str, file_name: str) -> List[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []
        with open(file_path) as f:
            for problem in linter.run(f, self._config):
                issues.append(
                    ValidationError(
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
