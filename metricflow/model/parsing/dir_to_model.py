import logging
import os
from dataclasses import dataclass
from string import Template
import traceback
import git
from typing import Optional, Dict, List, Union, Type

from jsonschema import exceptions

from metricflow.errors.errors import ParsingException
from metricflow.model.model_transformer import ModelTransformer
from metricflow.model.objects.common import Version, YamlConfigFile
from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.materialization import Materialization
from metricflow.model.objects.metric import Metric
from metricflow.model.parsing.schemas_internal import (
    metric_validator,
    data_source_validator,
    materialization_validator,
)
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.parsing.yaml_loader import (
    ParsingContext,
    YamlConfigLoader,
    PARSING_CONTEXT_KEY,
)
from metricflow.model.validations.validator_helpers import (
    FileContext,
    ModelValidationException,
    ModelValidationResults,
    ValidationError,
    ValidationIssueType,
)

logger = logging.getLogger(__name__)

VERSION_KEY = "mf_config_schema"
METRIC_TYPE = "metric"
DATA_SOURCE_TYPE = "data_source"
MATERIALIZATION_TYPE = "materialization"
DOCUMENT_TYPES = [METRIC_TYPE, DATA_SOURCE_TYPE, MATERIALIZATION_TYPE]


@dataclass(frozen=True)
class ModelBuildResult:  # noqa: D
    model: UserConfiguredModel
    # Issues found in the model.
    issues: ModelValidationResults = ModelValidationResults()


@dataclass(frozen=True)
class FileParsingResult:
    """Results of parsing a config file

    Attributes:
        elements: MetricFlow model elements parsed from the file
        issues: Issues found when trying to parse the file
    """

    elements: List[Union[DataSource, Metric, Materialization]]
    issues: List[ValidationIssueType]


def collect_yaml_config_file_paths(directory: str) -> List[str]:
    """Collects a list of file paths for model config files

    Ignores files that are:
        - In hidden directories (i.e. directories starting with '.')
        - Hidden files (i.e. files starting with '.')
        - Non YAML files
        - Ignored by the repo's .gitignore file (if a repo is detected)

    NOTE: We ignore files ignored by .gitignore because an issue cropped up wherein
    sometimes dependencies of projects include YAML files in their package.
    """
    config_file_paths: List[str] = []
    for root, dirs, files in os.walk(directory):
        # Skip hidden directories. os.walk() supports mutation of dirs to skip directories.
        dirs[:] = [d for d in dirs if not d.startswith(".")]

        for file in files:
            if not YamlConfigLoader.is_valid_yaml_file_ending(file):
                continue
            # Skip hidden files
            if file.startswith("."):
                continue

            file_path = os.path.join(root, file)
            config_file_paths.append(file_path)

    try:
        repo = git.Repo(directory, search_parent_directories=True)
        # repo.ignored returns a list of file paths which are the file paths
        # that should be ignored as a subset of the handed in file paths
        ignored_files = repo.ignored(config_file_paths)
        config_file_paths = list(set(config_file_paths) - set(ignored_files))
    except git.exc.InvalidGitRepositoryError:
        pass

    return config_file_paths


def parse_directory_of_yaml_files_to_model(
    directory: str,
    template_mapping: Optional[Dict[str, str]] = None,
    apply_pre_transformations: Optional[bool] = True,
    apply_post_transformations: Optional[bool] = True,
    raise_issues_as_exceptions: bool = True,
) -> ModelBuildResult:
    """Parse files in the given directory to a UserConfiguredModel.

    Strings in the file following the Python string template format are replaced according to the template_mapping dict.
    """
    file_paths = collect_yaml_config_file_paths(directory=directory)
    return parse_yaml_file_paths_to_model(
        file_paths=file_paths,
        template_mapping=template_mapping,
        apply_pre_transformations=apply_pre_transformations,
        apply_post_transformations=apply_post_transformations,
        raise_issues_as_exceptions=raise_issues_as_exceptions,
    )


def parse_yaml_file_paths_to_model(
    file_paths: List[str],
    template_mapping: Optional[Dict[str, str]] = None,
    apply_pre_transformations: Optional[bool] = True,
    apply_post_transformations: Optional[bool] = True,
    raise_issues_as_exceptions: bool = True,
) -> ModelBuildResult:
    """Parse files the given list of file paths to a UserConfiguredModel.

    Strings in the files following the Python string template format are replaced according to the template_mapping dict.
    """
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

    build_result = parse_yaml_files_to_model(yaml_config_files)
    model = build_result.model
    assert model

    build_issues = build_result.issues
    try:
        if apply_pre_transformations:
            model = ModelTransformer.pre_validation_transform_model(model)

        if apply_post_transformations:
            model = ModelTransformer.post_validation_transform_model(model)
    except Exception as e:
        transformation_issue_results = ModelValidationResults(errors=[ValidationError(message=str(e))])
        build_issues = ModelValidationResults.merge([build_issues, transformation_issue_results])

    if raise_issues_as_exceptions and build_issues.has_blocking_issues:
        raise ModelValidationException(build_issues.all_issues)

    return ModelBuildResult(model=model, issues=build_issues)


def parse_yaml_files_to_model(
    files: List[YamlConfigFile],
    data_source_class: Type[DataSource] = DataSource,
    metric_class: Type[Metric] = Metric,
    materialization_class: Type[Materialization] = Materialization,
) -> ModelBuildResult:
    """Builds UserConfiguredModel from list of config files (as strings).

    Persistent storage connection may be passed to write parsed objects=
    to storage and populate object metadata

    Note: this function does not finalize the model
    """
    data_sources = []
    metrics = []
    materializations: List[Materialization] = []
    valid_object_classes = [data_source_class.__name__, metric_class.__name__, materialization_class.__name__]
    issues: List[ValidationIssueType] = []

    for config_file in files:
        parsing_result = parse_config_yaml(  # parse config file
            config_file,
            data_source_class=data_source_class,
            metric_class=metric_class,
            materialization_class=materialization_class,
        )
        file_issues = parsing_result.issues
        for obj in parsing_result.elements:
            if isinstance(obj, data_source_class):
                data_sources.append(obj)
            elif isinstance(obj, metric_class):
                metrics.append(obj)
            elif isinstance(obj, materialization_class):
                materializations.append(obj)
            else:
                file_issues.append(
                    ValidationError(
                        context=FileContext(file_name=config_file.filepath),
                        message=f"Unexpected model object {obj.__name__}. Expected {valid_object_classes}.",
                    )
                )

        issues += file_issues

    return ModelBuildResult(
        model=UserConfiguredModel(
            data_sources=data_sources,
            materializations=materializations,
            metrics=metrics,
        ),
        issues=ModelValidationResults.from_issues_sequence(issues),
    )


def parse_config_yaml(
    config_yaml: YamlConfigFile,
    data_source_class: Type[DataSource] = DataSource,
    metric_class: Type[Metric] = Metric,
    materialization_class: Type[Materialization] = Materialization,
) -> FileParsingResult:
    """Parses transform config file passed as string - Returns list of model objects"""
    results: List[Union[DataSource, Metric, Materialization]] = []
    ctx: Optional[ParsingContext] = None
    issues: List[ValidationIssueType] = []
    try:
        for config_document in YamlConfigLoader.load_all_with_context(
            name=config_yaml.filepath, contents=config_yaml.contents
        ):
            # The config document can be None if there is nothing but white space between two `---`
            # this isn't really an issue, so lets just swallow it
            if config_document is None:
                continue
            if not isinstance(config_document, dict):
                issues.append(
                    ValidationError(
                        context=FileContext(file_name=config_yaml.filepath),
                        message=f"YAML must be a dict. Got `{type(config_document)}`.",
                    )
                )
                continue

            keys = config_document.keys()

            # This SHOULDN'T ever happen but if it does, we want to know. If this
            # does ever happen, it is likely due to a change in 'load_all_with_context'
            if PARSING_CONTEXT_KEY not in keys:
                raise RuntimeError(
                    f"No parsing context present. Expected key `{PARSING_CONTEXT_KEY}` from the YAML parser."
                )

            ctx = config_document.pop(PARSING_CONTEXT_KEY)
            assert ctx

            if VERSION_KEY in config_document:
                version = Version.parse(config_document.pop(VERSION_KEY))
                major_version = version.major

                if major_version != 0:
                    issues.append(
                        ValidationError(
                            context=FileContext(file_name=ctx.filename, line_number=ctx.start_line),
                            message=f"Unsupported version {version} in config document.",
                        )
                    )

            # Because we've popped the VERSION KEY and PARSING_CONTEXT_KEY, there
            # should only be the base object key remaining
            if len(keys) != 1:
                issues.append(
                    ValidationError(
                        context=FileContext(file_name=ctx.filename, line_number=ctx.start_line),
                        message=f"Document should have one type of key, but has {keys}.",
                    )
                )
                continue

            # retrieve last top-level key as type
            document_type = next(iter(config_document.keys()))
            object_cfg = config_document[document_type]

            try:
                if document_type == METRIC_TYPE:
                    metric_validator.validate(config_document[document_type])
                    results.append(metric_class.parse_obj(object_cfg))
                elif document_type == DATA_SOURCE_TYPE:
                    data_source_validator.validate(config_document[document_type])
                    results.append(data_source_class.parse_obj(object_cfg))
                elif document_type == MATERIALIZATION_TYPE:
                    materialization_validator.validate(config_document[document_type])
                    results.append(materialization_class.parse_obj(object_cfg))
                else:
                    issues.append(
                        ValidationError(
                            context=FileContext(file_name=ctx.filename, line_number=ctx.start_line),
                            message=f"Invalid document type: {document_type}. Expected {DOCUMENT_TYPES}.",
                        )
                    )
            # catches exceptions from jsonschema validator
            except exceptions.ValidationError as e:
                context = FileContext(file_name=ctx.filename, line_number=ctx.start_line)
                issues.append(
                    ValidationError(
                        context=context,
                        message=f"YAML document did not conform to metric spec.\nError: {e}",
                        extra_detail="".join(traceback.format_tb(e.__traceback__)),
                    )
                )
            # ParsingException: catches exceptions from *.parse_obj calls
            # Exception: general exception for a given document. Basicially we
            # don't want an exception on one document to halt checking the rest
            # of the documents
            except (ParsingException, Exception) as e:
                context = FileContext(file_name=ctx.filename, line_number=ctx.start_line)
                issues.append(
                    ValidationError(
                        context=context,
                        message=str(e),
                        extra_detail="".join(traceback.format_tb(e.__traceback__)),
                    )
                )
    # If a runtime error occured, we still want this to break things
    except RuntimeError:
        raise
    # Any other error should be handled as an issue
    except Exception as e:
        context = FileContext(file_name=config_yaml.filepath)
        issues.append(
            ValidationError(context=context, message=str(e), extra_detail="".join(traceback.format_tb(e.__traceback__)))
        )

    return FileParsingResult(elements=results, issues=issues)
