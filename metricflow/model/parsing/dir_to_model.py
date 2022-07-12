import logging
import os
import textwrap
from dataclasses import dataclass
from string import Template
from typing import Optional, Dict, List, Union, Type

from jsonschema import exceptions
from yaml.scanner import ScannerError

from metricflow.errors.errors import ParsingException
from metricflow.model.model_transformer import ModelTransformer
from metricflow.model.objects.common import Version, YamlConfigFile
from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.materialization import Materialization
from metricflow.model.objects.metric import Metric
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.parsing.validation import (
    validate_config_structure,
    VERSION_KEY,
    METRIC_TYPE,
    DATA_SOURCE_TYPE,
    MATERIALIZATION_TYPE,
    DOCUMENT_TYPES,
)
from metricflow.model.parsing.yaml_loader import (
    ParsingContext,
    YamlConfigLoader,
    PARSING_CONTEXT_KEY,
)
from metricflow.model.validations.validator_helpers import ModelValidationResults

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ModelBuildResult:  # noqa: D
    model: Optional[UserConfiguredModel] = None
    # Issues found in the model.
    issues: ModelValidationResults = ModelValidationResults()


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
            if not YamlConfigLoader.is_valid_yaml_file_ending(file):
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
    ctx: Optional[ParsingContext] = None
    errors = []
    try:
        # Validates that config yaml conforms to json schema
        validate_config_structure(config_yaml)

        for config_document in YamlConfigLoader.load_all_with_context(
            name=config_yaml.filepath, contents=config_yaml.contents
        ):
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

            if document_type == METRIC_TYPE:
                results.append(metric_class.parse_obj(object_cfg))
            elif document_type == DATA_SOURCE_TYPE:
                results.append(data_source_class.parse_obj(object_cfg))
            elif document_type == MATERIALIZATION_TYPE:
                results.append(materialization_class.parse_obj(object_cfg))
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
