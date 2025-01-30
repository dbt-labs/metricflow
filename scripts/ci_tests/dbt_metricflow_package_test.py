from __future__ import annotations

import subprocess
import textwrap
from pathlib import Path
from typing import Optional


def _run_shell_command(command: str, cwd: Optional[Path] = None) -> None:
    if cwd is None:
        cwd = Path.cwd()

    print(
        textwrap.dedent(
            f"""\
            Running via shell:
                command: {command!r}
                cwd: {cwd.as_posix()!r}
            """
        ).rstrip()
    )
    subprocess.check_call(command, shell=True, cwd=cwd.as_posix())


if __name__ == "__main__":
    # Check that the `mf` command is installed.
    _run_shell_command("which python")
    _run_shell_command("which mf")
    _run_shell_command("mf")
    # Run the tutorial using `--yes` to create the sample project without user interaction.
    _run_shell_command("mf tutorial --yes")
    tutorial_directory = Path.cwd().joinpath("mf_tutorial_project")

    # Run the first few tutorial steps.
    _run_shell_command("dbt seed", cwd=tutorial_directory)
    _run_shell_command("dbt build", cwd=tutorial_directory)
    _run_shell_command(
        "mf query --metrics transactions --group-by metric_time --order metric_time",
        cwd=tutorial_directory,
    )
