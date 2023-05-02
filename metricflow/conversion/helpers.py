import os
import subprocess


def found_dbt_project() -> str:
    """Checks if dbt_project.yml exists in the current directory and returns a boolean."""
    try:
        os.path.exists("dbt_project.yml")
        return "Completion: Found the dbt project file!"
    except:
        return "Error: dbt_project.yml not found in current directory. Please navigate to a dbt directory to run conversion"

def run_dbt_compile() -> None:
    """Runs the dbt compile command."""
    ## TODO: Make this a better error message and account for more error paths
    try:
        subprocess.run("dbt compile", shell=True, check=True)
        return "Completion: dbt compile ran successfully."
    except FileNotFoundError:
        return "Error: dbt not found. Please make sure dbt is installed and available in the system path."
    except subprocess.CalledProcessError as e:
        return f"Error: dbt compile failed with exit code {e.returncode}."