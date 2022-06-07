import inspect
import logging
import os.path
import textwrap
from dataclasses import dataclass
from string import Template
from typing import Optional, Dict, List, Union, Type, Any, Tuple

import yaml
from jsonschema import exceptions
from yaml.scanner import ScannerError

from metricflow.errors.errors import ParsingException
from metricflow.model.model_transformer import ModelTransformer
from metricflow.model.objects.common import Version, YamlConfigFile
from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.materialization import Materialization
from metricflow.model.objects.metric import Metric
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.objects.utils import ParseableObject, ParseableField
from metricflow.model.parsing.validation import (
    validate_config_structure,
    VERSION_KEY,
    METRIC_TYPE,
    DATA_SOURCE_TYPE,
    MATERIALIZATION_TYPE,
    DOCUMENT_TYPES,
)
from metricflow.model.parsing.yaml_loader import ParsingContext, SafeLineLoader, PARSING_CONTEXT_KEY
from metricflow.model.validations.validator_helpers import ValidationIssueType

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ModelBuildResult:  # noqa: D
    model: Optional[UserConfiguredModel] = None
    # Issues found in the model.
    issues: Tuple[ValidationIssueType, ...] = tuple()


def parse_directory_of_yaml_files_to_model(
    directory: str,
    template_mapping: Optional[Dict[str, str]] = None,
    apply_pre_transformations: Optional[bool] = True,
    apply_post_transformations: Optional[bool] = True,
) -> ModelBuildResult:
    """Parse files in the given directory to a TMdoModel.

    Strings in the file following the Python string template format are replaced according to the template_mapping dict.
    """
    template_mapping = template_mapping or {}
    yaml_config_files = []

    for root, dirs, files in os.walk(directory):
        # Skip hidden directories. os.walk() supports mutation of dirs to skip directories.
        dirs[:] = [d for d in dirs if not d.startswith(".")]

        for file in files:
            if not (file.endswith(".yaml") or file.endswith(".yml")):
                continue
            # Skip hidden files
            if file.startswith("."):
                continue

            file_path = os.path.join(root, file)
            with open(file_path) as f:
                contents = Template(f.read()).substitute(template_mapping)
                yaml_config_files.append(
                    YamlConfigFile(filepath=file_path, contents=contents),
                )

    model = parse_yaml_files_to_model(yaml_config_files)

    if apply_pre_transformations:
        model = ModelTransformer.pre_validation_transform_model(model)

    if apply_post_transformations:
        model = ModelTransformer.post_validation_transform_model(model)

    return ModelBuildResult(model=model)


def parse_yaml_files_to_model(
    files: List[YamlConfigFile],
    data_source_class: Type[DataSource] = DataSource,
    metric_class: Type[Metric] = Metric,
    materialization_class: Type[Materialization] = Materialization,
) -> UserConfiguredModel:
    """Builds UserConfiguredModel from list of config files (as strings).

    Persistent storage connection may be passed to write parsed objects=
    to storage and populate object metadata

    Note: this function does not finalize the model
    """
    data_sources = []
    metrics = []
    materializations: List[Materialization] = []
    valid_object_classes = [data_source_class.__name__, metric_class.__name__, materialization_class.__name__]
    for config_file in files:
        objects = parse_config_yaml(  # parse config file
            config_file,
            data_source_class=data_source_class,
            metric_class=metric_class,
            materialization_class=materialization_class,
        )
        for obj in objects:
            if isinstance(obj, data_source_class):
                data_sources.append(obj)
            elif isinstance(obj, metric_class):
                metrics.append(obj)
            elif isinstance(obj, materialization_class):
                materializations.append(obj)
            else:
                raise ParsingException(
                    f"Unexpected model object {obj.__name__}. Expected {valid_object_classes}.",
                    config_filepath=config_file.filepath,
                )

    return UserConfiguredModel(
        data_sources=data_sources,
        materializations=materializations,
        metrics=metrics,
    )


def parse_config_yaml(
    config_yaml: YamlConfigFile,
    data_source_class: Type[DataSource] = DataSource,
    metric_class: Type[Metric] = Metric,
    materialization_class: Type[Materialization] = Materialization,
) -> List[Union[DataSource, Metric, Materialization]]:
    """Parses transform config file passed as string - Returns list of model objects"""
    results: List[Union[DataSource, Metric, Materialization]] = []
    filename: str = os.path.split(config_yaml.filepath)[-1]
    ctx: Optional[ParsingContext] = None
    errors = []
    try:
        # Validates that config yaml conforms to json schema
        validate_config_structure(config_yaml)

        for config_document in yaml.load_all(config_yaml.contents, Loader=SafeLineLoader):
            # The config document can be None if there is nothing but white space between two `---`
            # this isn't really an issue, so lets just swallow it
            if config_document is None:
                continue
            if not isinstance(config_document, dict):
                errors.append(
                    str(
                        ParsingException(
                            f"YAML must be a dict. Got `{type(config_document)}`.",
                            config_filepath=config_yaml.filepath,
                        )
                    )
                )
                continue
            keys = config_document.keys()
            if PARSING_CONTEXT_KEY not in keys:
                raise RuntimeError(
                    f"No parsing context present. Expected key `{PARSING_CONTEXT_KEY}` from the YAML parser."
                )

            ctx = config_document.pop(PARSING_CONTEXT_KEY)
            assert ctx
            ctx.filename = filename

            if VERSION_KEY in config_document:
                version = Version.parse(config_document.pop(VERSION_KEY))
                major_version = version.major

                if major_version != 0:
                    errors.append(
                        str(
                            ParsingException(
                                f"Unsupported version {version} in config document.",
                                config_filepath=config_yaml.filepath,
                            )
                        )
                    )

            if len(keys) != 1:
                errors.append(
                    str(
                        ParsingException(
                            f"Document should have one type of key, but has {keys}.",
                            ctx=ctx,
                            config_filepath=config_yaml.filepath,
                        )
                    )
                )
                continue

            # retrieve last top-level key as type
            document_type = next(iter(config_document.keys()))
            object_cfg = config_document[document_type]
            yaml_contents_by_line = config_yaml.contents.splitlines()

            # Add filepath to the object context
            object_cfg[PARSING_CONTEXT_KEY].filename = config_yaml.filepath

            if document_type == METRIC_TYPE:
                results.append(parse(metric_class, object_cfg, config_yaml.filepath, yaml_contents_by_line))
            elif document_type == DATA_SOURCE_TYPE:
                results.append(parse(data_source_class, object_cfg, config_yaml.filepath, yaml_contents_by_line))
            elif document_type == MATERIALIZATION_TYPE:
                results.append(parse(materialization_class, object_cfg, config_yaml.filepath, yaml_contents_by_line))
            else:
                errors.append(
                    str(
                        ParsingException(
                            message=f"Invalid document type: {document_type}. Expected {DOCUMENT_TYPES}.",
                            ctx=ctx,
                            config_filepath=config_yaml.filepath,
                        )
                    )
                )

        if len(errors) > 0:
            errors_str = "\n".join([str(x) for x in errors])
            raise ParsingException(
                message=f"Found {len(errors)} error(s) parsing configs files:\n"
                f"{textwrap.indent(errors_str, prefix='    ')}"
            )
    except exceptions.ValidationError as e:
        raise ParsingException(
            message=f"YAML file did not conform to metric spec.\nError: {e}",
            ctx=ctx,
            config_filepath=config_yaml.filepath,
        ) from e
    except ScannerError as e:
        raise ParsingException(
            message=str(e),
            ctx=ctx,
            config_filepath=config_yaml.filepath,
        ) from e
    except ParsingException:
        raise
    except Exception as e:
        raise ParsingException(
            message=str(e),
            ctx=ctx,
            config_filepath=config_yaml.filepath,
        ) from e

    return results


def parse(  # type: ignore[misc]
    _type: Type[Union[DataSource, Metric, Materialization]],
    yaml_dict: Dict[str, Any],
    filepath: str,
    contents_by_line: List[str],
) -> Any:
    """Parses a model object from (jsonschema-validated) yaml into python object"""

    # Add Metadata
    ctx = yaml_dict.pop(PARSING_CONTEXT_KEY)
    filename = os.path.split(filepath)[-1]
    yaml_dict["metadata"] = {
        "repo_file_path": filepath,
        "file_slice": {
            "filename": filename,
            "content": "\n".join(contents_by_line[max(0, ctx.start_line - 1) : ctx.end_line]),
            "start_line_number": ctx.start_line,
            "end_line_number": ctx.end_line,
        },
    }

    for field_name, field_value in _type.__fields__.items():
        if field_name in yaml_dict:
            if not inspect.isclass(field_value.type_):  # this handles the nested generic-type case (eg List[List[str]])
                continue
            if issubclass(field_value.type_, ParseableObject):
                if isinstance(yaml_dict[field_name], list):
                    objects = []
                    for obj in yaml_dict[field_name]:
                        objects.append(parse(field_value.type_, obj, filepath, contents_by_line))  # type: ignore
                    yaml_dict[field_name] = objects
                else:
                    yaml_dict[field_name] = parse(field_value.type_, yaml_dict[field_name], filepath, contents_by_line)  # type: ignore
            elif issubclass(field_value.type_, ParseableField):
                if isinstance(yaml_dict[field_name], list):
                    objects = []
                    for obj in yaml_dict[field_name]:
                        objects.append(field_value.type_.parse(obj))
                    yaml_dict[field_name] = objects
                else:
                    yaml_dict[field_name] = field_value.type_.parse(yaml_dict[field_name])

    return _type(**yaml_dict)
