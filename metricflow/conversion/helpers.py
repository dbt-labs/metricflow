import os

def found_dbt_project() -> bool:
    """Checks if dbt_project.yml exists in the current directory and returns a boolean."""
    return os.path.exists("dbt_project.yml")