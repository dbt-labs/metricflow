from __future__ import annotations

import subprocess
from pathlib import Path

if __name__ == "__main__":
    # Check that the `mf` command is installed.
    print(f"Running from {Path.cwd().as_posix()}")
    print("Checking path to python")
    subprocess.check_call("which python", shell=True)
    subprocess.check_call("mf", shell=True)
    # Run the tutorial using `--yes` to create the sample project without user interaction.
    subprocess.check_call("mf tutorial --yes", shell=True)
    tutorial_directory = Path.cwd().joinpath("mf_tutorial_project").as_posix()

    # Run the first few tutorial steps.
    subprocess.check_call("dbt seed", cwd=tutorial_directory, shell=True)
    subprocess.check_call("dbt build", cwd=tutorial_directory, shell=True)
    subprocess.check_call(
        "mf query --metrics transactions --group-by metric_time --order metric_time", cwd=tutorial_directory, shell=True
    )
