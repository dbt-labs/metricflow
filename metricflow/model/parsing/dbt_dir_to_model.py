from metricflow.model.parsing.dir_to_model import ModelBuildResult


def parse_dbt_project_to_model(
    directory: str,
) -> ModelBuildResult:
    """Parse dbt model files in the given directory to a UserConfiguredModel."""
    raise NotImplementedError(
        f"Unable to parse dbt project at {directory} to a UserConfiguredModel, because we haven't implemented it"
    )
