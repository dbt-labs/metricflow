import datetime as dt

from dateutil.parser import parse
from typing import Optional

from metricflow.configuration.constants import CONFIG_MODEL_PATH
from metricflow.configuration.yaml_handler import YamlFileHandler
from metricflow.errors.errors import ModelCreationException
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.parsing.dir_to_model import ModelBuildResult, parse_directory_of_yaml_files_to_model
from metricflow.sql_clients.common_client import not_empty


def path_to_models(handler: YamlFileHandler) -> str:
    """Given a YamlFileHandler, return the path to the YAML model config files"""
    return not_empty(handler.get_value(CONFIG_MODEL_PATH), CONFIG_MODEL_PATH, handler.url)


def model_build_result_from_config(
    handler: YamlFileHandler, raise_issues_as_exceptions: bool = True
) -> ModelBuildResult:
    """Given a yaml file, creates a ModelBuildResult.

    Args:
        handler: a file handler for loading the configs from
        raise_issues_as_exceptions: determines if issues should be raised, or returned as issues

    Returns:
        ModelBuildResult that contains the UserConfigureModel and any associated ValidationIssues
    """
    models_path = path_to_models(handler=handler)
    try:
        return parse_directory_of_yaml_files_to_model(
            models_path, raise_issues_as_exceptions=raise_issues_as_exceptions
        )
    except Exception as e:
        raise ModelCreationException from e


def model_build_result_from_dbt_config(
    handler: YamlFileHandler,
    raise_issues_as_exceptions: bool = True,
    profile: Optional[str] = None,
    target: Optional[str] = None,
) -> ModelBuildResult:
    """Given a yaml file, creates a ModelBuildResult.

    Args:
        handler: a file handler for loading the configs from
        raise_issues_as_exceptions: determines if issues should be raised, or returned as issues
        profile: a dbt profile to override the project default, default None
        target: a dbt target to overide the profile default, default None

    Returns:
        ModelBuildResult that contains the UserConfigureModel and any associated ValidationIssues
    """
    dbt_models_path = path_to_models(handler=handler)
    try:
        # This import results in eventually importing dbt, and dbt is an
        # optional dep meaning it isn't guaranteed to be installed. If the
        # import is at the top ofthe file MetricFlow will blow up if dbt
        # isn't installed. Thus by importing it here, we only run into the
        # exception if this method is called without dbt installed.
        from metricflow.model.parsing.dbt_dir_to_model import parse_dbt_project_to_model

        return parse_dbt_project_to_model(directory=dbt_models_path, profile=profile, target=target)
    except Exception as e:
        raise ModelCreationException from e


def build_user_configured_model_from_config(handler: YamlFileHandler) -> UserConfiguredModel:
    """Given a yaml file, create a UserConfiguredModel."""
    return model_build_result_from_config(handler=handler).model


def build_user_configured_model_from_dbt_config(
    handler: YamlFileHandler, profile: Optional[str] = None, target: Optional[str] = None
) -> UserConfiguredModel:
    """Given a yaml file, create a UserConfiguredModel."""
    return model_build_result_from_dbt_config(handler=handler, profile=profile, target=target).model


def convert_to_datetime(datetime_str: Optional[str]) -> Optional[dt.datetime]:
    """Callback to convert string to datetime given as an iso8601 timestamp."""
    if datetime_str is None:
        return None

    try:
        return parse(datetime_str)
    except Exception:
        raise ValueError(f"'{datetime_str}' is not a valid iso8601 timestamp")
