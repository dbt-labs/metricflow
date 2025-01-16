from __future__ import annotations

import subprocess

import metricflow_semantics  # noqa: F401

# Check that modules can be imported
import metricflow  # noqa: F401

if __name__ == "__main__":
    # Check that the `mf` command is installed.
    subprocess.check_call("venv/bin/mf", shell=True)
