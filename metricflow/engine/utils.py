from metricflow.configuration.constants import CONFIG_MODEL_PATH
from metricflow.configuration.yaml_handler import YamlFileHandler
from metricflow.errors.errors import ModelCreationException
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.parsing.dir_to_model import parse_directory_of_yaml_files_to_model


def build_user_configured_model_from_config(handler: YamlFileHandler) -> UserConfiguredModel:
    """Given a yaml file, create a UserConfiguredModel."""
    path_to_models = handler.get_value(CONFIG_MODEL_PATH)
    try:
        model = parse_directory_of_yaml_files_to_model(path_to_models).model
        assert model is not None
        return model
    except Exception as e:
        raise ModelCreationException from e
