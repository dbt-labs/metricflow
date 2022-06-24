import datetime as dt

from dateutil.parser import parse
from typing import Optional

from metricflow.configuration.constants import CONFIG_MODEL_PATH
from metricflow.configuration.yaml_handler import YamlFileHandler
from metricflow.errors.errors import ModelCreationException
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.parsing.dir_to_model import parse_directory_of_yaml_files_to_model
from metricflow.sql_clients.common_client import not_empty


def path_to_models(handler: YamlFileHandler) -> str:
    """Given a YamlFileHandler, return the path to the YAML model config files"""
    return not_empty(handler.get_value(CONFIG_MODEL_PATH), CONFIG_MODEL_PATH, handler.url)


def build_user_configured_model_from_config(handler: YamlFileHandler) -> UserConfiguredModel:
    """Given a yaml file, create a UserConfiguredModel."""
    models_path = path_to_models(handler=handler)
    try:
        model = parse_directory_of_yaml_files_to_model(models_path).model
        assert model is not None
        return model
    except Exception as e:
        raise ModelCreationException from e


def convert_to_datetime(datetime_str: Optional[str]) -> Optional[dt.datetime]:
    """Callback to convert string to datetime given as an iso8601 timestamp."""
    if datetime_str is None:
        return None

    try:
        return parse(datetime_str)
    except Exception:
        raise ValueError(f"'{datetime_str}' is not a valid iso8601 timestamp")
