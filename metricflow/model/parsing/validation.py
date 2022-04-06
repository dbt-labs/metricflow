import logging

import yaml
from jsonschema import exceptions

from metricflow.errors.errors import ParsingException
from metricflow.model.parsing.yaml_file import YamlFile
from metricflow.model.parsing.schemas_internal import (
    metric_validator,
    data_source_validator,
    materialization_validator,
)

VERSION_KEY = "mf_config_schema"
METRIC_TYPE = "metric"
DATA_SOURCE_TYPE = "data_source"
MATERIALIZATION_TYPE = "materialization"
DOCUMENT_TYPES = [METRIC_TYPE, DATA_SOURCE_TYPE, MATERIALIZATION_TYPE]

logger = logging.getLogger(__name__)


def validate_config_structure(config_yaml: YamlFile) -> None:  # noqa: D
    """Validates config shape against jsonschema

    catches ValidationError and raise one exception at the end so
    we can get all the validation errors rather than just the first
    """
    errors = []
    for config_document in yaml.load_all(config_yaml.contents, Loader=yaml.SafeLoader):
        # The config document can be None if there is nothing but white space between two `---`
        # this isn't really an issue, so lets just swallow it
        if config_document is None:
            continue
        if not isinstance(config_document, dict):
            errors.append(
                str(
                    ParsingException(
                        f"Document is not a dict. Got `{type(config_document)}`: {config_document}",
                        config_yaml=config_yaml,
                    )
                )
            )
            continue
        for document_type in config_document.keys():
            try:
                if document_type == METRIC_TYPE:
                    metric_validator.validate(config_document[document_type])
                elif document_type == DATA_SOURCE_TYPE:
                    data_source_validator.validate(config_document[document_type])
                elif document_type == MATERIALIZATION_TYPE:
                    materialization_validator.validate(config_document[document_type])
                elif document_type == VERSION_KEY:
                    pass
                else:
                    raise ParsingException(
                        f"Invalid document type '{document_type}'. Valid document types are: {DOCUMENT_TYPES}",
                        config_yaml=config_yaml,
                    )
            except exceptions.ValidationError as e:
                errors.append(f"{e}")
                logger.exception(str(e))

    if len(errors) > 0:
        raise exceptions.ValidationError("\n".join(errors))
