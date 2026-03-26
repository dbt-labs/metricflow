from __future__ import annotations

import logging
import os
import traceback
from dataclasses import dataclass
from string import Template
from typing import Dict, List, Optional, Sequence, Type, Union

from jsonschema import exceptions

from metricflow_semantic_interfaces.errors import ParsingException
from metricflow_semantic_interfaces.implementations.element_config import (
    PydanticSemanticLayerElementConfig,
)
from metricflow_semantic_interfaces.implementations.elements.dimension import PydanticDimension
from metricflow_semantic_interfaces.implementations.elements.entity import PydanticEntity
from metricflow_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from metricflow_semantic_interfaces.implementations.metric import PydanticMetric
from metricflow_semantic_interfaces.implementations.project_configuration import (
    PydanticProjectConfiguration,
)
from metricflow_semantic_interfaces.implementations.saved_query import PydanticSavedQuery
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from metricflow_semantic_interfaces.parsing.objects import Version, YamlConfigFile
from metricflow_semantic_interfaces.parsing.schemas import (
    metric_validator,
    project_configuration_validator,
    saved_query_validator,
    semantic_model_validator,
)
from metricflow_semantic_interfaces.parsing.yaml_loader import (
    PARSING_CONTEXT_KEY,
    ParsingContext,
    YamlConfigLoader,
)
from metricflow_semantic_interfaces.pretty_print import pformat_big_objects
from metricflow_semantic_interfaces.transformations.semantic_manifest_transformer import (
    PydanticSemanticManifestTransformer,
)
from metricflow_semantic_interfaces.validations.validator_helpers import (
    FileContext,
    SemanticManifestValidationException,
    SemanticManifestValidationResults,
    ValidationError,
    ValidationIssue,
)

logger = logging.getLogger(__name__)

VERSION_KEY = "mf_config_schema"
METRIC_TYPE = "metric"
SEMANTIC_MODEL_TYPE = "semantic_model"
PROJECT_CONFIGURATION_TYPE = "project_configuration"
SAVED_QUERY_TYPE = "saved_query"

DOCUMENT_TYPES = [METRIC_TYPE, SEMANTIC_MODEL_TYPE, PROJECT_CONFIGURATION_TYPE, SAVED_QUERY_TYPE]


@dataclass(frozen=True)
class SemanticManifestBuildResult:  # noqa: D101
    semantic_manifest: PydanticSemanticManifest
    # Issues found in the model.
    issues: SemanticManifestValidationResults = SemanticManifestValidationResults()


@dataclass(frozen=True)
class FileParsingResult:
    """Results of parsing a config file.

    Attributes:
        elements: MetricFlow model elements parsed from the file
        issues: Issues found when trying to parse the file
    """

    elements: List[Union[PydanticSemanticModel, PydanticMetric, PydanticProjectConfiguration, PydanticSavedQuery]]
    issues: List[ValidationIssue]


def collect_yaml_config_file_paths(directory: str) -> List[str]:
    """Collects a list of file paths for model config files.

    Ignores files that are:
        - In hidden directories (i.e. directories starting with '.')
        - Hidden files (i.e. files starting with '.')
        - Non YAML files
        - Ignored by the repo's .gitignore file (if a repo is detected)
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

    return config_file_paths


def parse_directory_of_yaml_files_to_semantic_manifest(
    directory: str,
    template_mapping: Optional[Dict[str, str]] = None,
    apply_transformations: Optional[bool] = True,
    raise_issues_as_exceptions: bool = True,
) -> SemanticManifestBuildResult:
    """Parse files in the given directory to a SemanticManifest.

    Strings in the file following the Python string template format are replaced
    according to the template_mapping dict.
    """
    file_paths = collect_yaml_config_file_paths(directory=directory)
    return parse_yaml_file_paths_to_semantic_manifest(
        file_paths=file_paths,
        template_mapping=template_mapping,
        apply_transformations=apply_transformations,
        raise_issues_as_exceptions=raise_issues_as_exceptions,
    )


def parse_yaml_file_paths_to_semantic_manifest(
    file_paths: List[str],
    template_mapping: Optional[Dict[str, str]] = None,
    apply_transformations: Optional[bool] = True,
    raise_issues_as_exceptions: bool = True,
) -> SemanticManifestBuildResult:
    """Parse files the given list of file paths to a SemanticManifest.

    Strings in the files following the Python string template format are replaced
    according to the template_mapping dict.
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
            # exception is hit *before* building the semantic manifest. Currently, the
            # SemanticManifestBuildResult guarantees a SemanticManifest. We could make
            # SemanticManifest optional on ModelBuildResult, but this has
            # undesirable consequences.
            raise Exception(
                f"The content of file `{file_path}` doesn't match the encoding of the file."
                " If you know the encoding the content is in, try resaving the file with that encoding explicitly."
                " Alternatively this error generally arises due to copy and pasted content,"
                " try manually typing up the problem file instead of copy and pasting"
            ) from e

    return parse_yaml_files_to_validation_ready_semantic_manifest(
        yaml_config_files=yaml_config_files,
        apply_transformations=apply_transformations,
        raise_issues_as_exceptions=raise_issues_as_exceptions,
    )


def parse_yaml_files_to_validation_ready_semantic_manifest(
    yaml_config_files: List[YamlConfigFile],
    apply_transformations: Optional[bool] = True,
    raise_issues_as_exceptions: bool = True,
) -> SemanticManifestBuildResult:
    """Parse and transform the given set of in-memory YamlConfigFiles to a UserConfigured model.

    This model result is, by default, validation-ready, although different callsites (mainly in testing)
    might wish to override the transformation state.

    TODO: Restructure this module and provide an improved API for managing these different input types
    """
    build_result = parse_yaml_files_to_semantic_manifest(yaml_config_files)
    model = build_result.semantic_manifest
    assert model

    build_issues = build_result.issues
    try:
        if apply_transformations:
            model = PydanticSemanticManifestTransformer.transform(model)
    except Exception as e:
        transformation_issue_results = SemanticManifestValidationResults(errors=(ValidationError(message=str(e)),))
        build_issues = SemanticManifestValidationResults.merge([build_issues, transformation_issue_results])

    if raise_issues_as_exceptions and build_issues.has_blocking_issues:
        raise SemanticManifestValidationException(build_issues.all_issues)

    return SemanticManifestBuildResult(semantic_manifest=model, issues=build_issues)


def parse_yaml_files_to_semantic_manifest(
    files: List[YamlConfigFile],
    semantic_model_class: Type[PydanticSemanticModel] = PydanticSemanticModel,
    metric_class: Type[PydanticMetric] = PydanticMetric,
    project_configuration_class: Type[PydanticProjectConfiguration] = PydanticProjectConfiguration,
    saved_query_class: Type[PydanticSavedQuery] = PydanticSavedQuery,
) -> SemanticManifestBuildResult:
    """Builds SemanticManifest from list of config files (as strings).

    Persistent storage connection may be passed to write parsed objects=
    to storage and populate object metadata

    Note: this function does not finalize the model
    """
    semantic_models = []
    metrics = []
    project_configurations = []
    saved_queries = []

    valid_object_classes = [
        semantic_model_class.__name__,
        metric_class.__name__,
        project_configuration_class.__name__,
        saved_query_class.__name__,
    ]
    issues: List[ValidationIssue] = []

    for config_file in files:
        parsing_result = parse_config_yaml(  # parse config file
            config_file,
            semantic_model_class=semantic_model_class,
            metric_class=metric_class,
            project_configuration_class=project_configuration_class,
            saved_query_class=saved_query_class,
        )
        file_issues = parsing_result.issues
        for obj in parsing_result.elements:
            if isinstance(obj, semantic_model_class):
                semantic_models.append(obj)
            elif isinstance(obj, metric_class):
                metrics.append(obj)
            elif isinstance(obj, project_configuration_class):
                project_configurations.append(obj)
            elif isinstance(obj, saved_query_class):
                saved_queries.append(obj)
            else:
                file_issues.append(
                    ValidationError(
                        context=FileContext(file_name=config_file.filepath),
                        message=f"Unexpected model object {obj.__class__.__name__}. Expected {valid_object_classes}.",
                    )
                )

        issues += file_issues

    if len(project_configurations) != 1:
        raise ParsingException(
            f"Did not find exactly one project configuration. Debugging context is:\n\n"
            f"{pformat_big_objects(project_configurations=project_configurations, issues=issues)}"
        )

    return SemanticManifestBuildResult(
        semantic_manifest=PydanticSemanticManifest(
            semantic_models=semantic_models,
            metrics=metrics,
            project_configuration=project_configurations[0],
            saved_queries=saved_queries,
        ),
        issues=SemanticManifestValidationResults.from_issues_sequence(issues),
    )


def parse_config_yaml(
    config_yaml: YamlConfigFile,
    semantic_model_class: Type[PydanticSemanticModel] = PydanticSemanticModel,
    metric_class: Type[PydanticMetric] = PydanticMetric,
    project_configuration_class: Type[PydanticProjectConfiguration] = PydanticProjectConfiguration,
    saved_query_class: Type[PydanticSavedQuery] = PydanticSavedQuery,
) -> FileParsingResult:
    """Parses transform config file passed as string - Returns list of model objects."""
    results: List[Union[PydanticSemanticModel, PydanticMetric, PydanticProjectConfiguration, PydanticSavedQuery]] = []
    ctx: Optional[ParsingContext] = None
    issues: List[ValidationIssue] = []
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
                elif document_type == SEMANTIC_MODEL_TYPE:
                    semantic_model_validator.validate(config_document[document_type])
                    sm = semantic_model_class.parse_obj(object_cfg)
                    # Combine configs according to the behavior documented here https://docs.getdbt.com/reference/configs-and-properties#combining-configs
                    elements: Sequence[Union[PydanticDimension, PydanticEntity, PydanticMeasure]] = [
                        *sm.dimensions,
                        *sm.entities,
                        *sm.measures,
                    ]
                    for element in elements:
                        if sm.config is not None:
                            if element.config is None:
                                element.config = PydanticSemanticLayerElementConfig(meta=sm.config.meta)
                            else:
                                element.config.meta = {**sm.config.meta, **element.config.meta}
                    results.append(sm)
                elif document_type == PROJECT_CONFIGURATION_TYPE:
                    project_configuration_validator.validate(config_document[document_type])
                    results.append(project_configuration_class.parse_obj(object_cfg))
                elif document_type == SAVED_QUERY_TYPE:
                    saved_query_validator.validate(config_document[document_type])
                    results.append(saved_query_class.parse_obj(object_cfg))
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
    # Any other error should be handled as an issue  # type: ignore[misc]
    except Exception as e:
        context = FileContext(file_name=config_yaml.filepath)
        issues.append(
            ValidationError(context=context, message=str(e), extra_detail="".join(traceback.format_tb(e.__traceback__)))
        )

    return FileParsingResult(elements=results, issues=issues)
