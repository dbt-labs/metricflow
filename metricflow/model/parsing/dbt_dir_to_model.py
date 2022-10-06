from dataclasses import dataclass
from enum import Enum
from pipes import Template
from typing import Dict, List, Optional
from metricflow.model.objects.common import YamlConfigFile
from metricflow.model.parsing.dir_to_model import ModelBuildResult, collect_yaml_config_file_paths
from metricflow.model.validations.validator_helpers import FileContext, ModelValidationResults, ValidationError, ValidationIssueType


class CalculationMethod(Enum):  # noqa: D
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    SUM = "sum"
    AVERAGE = "average"
    MIN = "min"
    MAX = "max"
    DERIVED = "derived"
    # 'expresision' is being deprecated for 'derived'
    # thus we will support both
    EXPRESSION = "expression"


class TimeGrains(Enum):  # noqa: D
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    YEAR = "year"


class Window:  # noqa: D
    count: int
    period: TimeGrains


class Filter:  # noqa: D
    field: str
    operator: str
    value: str


@dataclass(frozen=True)
class DbtMetric:  # noqa: D
    name: str
    model: Optional[str]
    label: Optional[str]
    description: Optional[str]
    calculation_method: Optional[CalculationMethod]
    # 'type' is being deprecated for 'calculation_method'.
    # thus we will support both, and ensure one exists
    type: Optional[CalculationMethod]
    expression: Optional[str]
    # 'sql' is being deprecated for 'expression'.
    # thus we will support both, and ensure one exists
    sql: Optional[str]
    timestamp: str
    time_grains: List[TimeGrains]
    dimensions: Optional[List[str]]
    window: Optional[Window]
    filters: Optional[List[Filter]]
    meta: Dict


def parse_dbt_project_to_model(
    directory: str,
    template_mapping: Optional[Dict[str, str]] = None,
    raise_issues_as_exceptions: bool = True,
) -> ModelBuildResult:
    """Parse dbt model files in the given directory to a UserConfiguredModel."""
    file_paths = collect_yaml_config_file_paths(directory=directory)
    template_mapping = template_mapping or {}
    yaml_config_files = []
    for file_path in file_paths:
        try:
            with open(file_path) as f:
                contents = Template(f.read()).substitute(template_mapping)
                yaml_config_files.append(
                    YamlConfigFile(filepath=file_path, contents=contents),
                )
        except UnicodeDecodeError as e:
            # We could alternatively return this as a validation issue, but this
            # exception is hit *before* building the model. Currently the
            # ModelBuildResult guarantees a UserConfiguredModel. We could make
            # UserConfiguredModel optional on ModelBuildResult, but this has
            # undesirable consequences.
            raise Exception(
                f"The content of file `{file_path}` doesn't match the encoding of the file."
                " If you know the encoding the content is in, try resaving the file with that encoding explicitly."
                " Alternatively this error generally arises due to copy and pasted content,"
                " try manually typing up the problem file instead of copy and pasting"
            ) from e

    dbt_metrics = build_dbt_metrics_from_yamls()
    raise NotImplementedError(
        f"Unable to parse dbt project at {directory} to a UserConfiguredModel, because we haven't implemented it"
    )
    

@dataclass(frozen=True)
class DbtMetricsParsingResult:  # noqa: D
    dbt_metics: List[DbtMetric]
    # Issues found in the model.
    issues: ModelValidationResults = ModelValidationResults()


def build_dbt_metrics_from_yamls(files: List[YamlConfigFile]) -> DbtMetricsParsingResult:
    metrics = []
    issues: List[ValidationIssueType] = []
    for config_file in files:
        parsing_result = parse_dbt_config_yaml(config_file)
        file_issues = parsing_result.issues
        for obj in parsing_result.elements:
            if isinstance(obj, DbtMetric):
                metrics.append(obj)
            else:
                file_issues.append(
                    ValidationError(
                        context=FileContext(file_name=config_file.filepath),
                        message=f"Unexpected model object `{obj.__name__}`. Expected `{DbtMetric.__name__}`.",
                    )
                )
        issues += file_issues
    return DbtMetricsParsingResult(
        metrics=metrics,
        issues=issues,
    )

@dataclass(frozen=True)
class DbtFileParsingResult:
    """Results of parsing a config file

    Attributes:
        elements: MetricFlow model elements parsed from the file
        issues: Issues found when trying to parse the file
    """

    elements: List[DbtMetric]
    issues: List[ValidationIssueType]


def parse_dbt_config_yaml(config_yaml: YamlConfigFile) -> DbtFileParsingResult:
    pass
