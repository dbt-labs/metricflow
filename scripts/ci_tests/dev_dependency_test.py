"""Rough test to check that dev dependencies between `metricflow` and `metricflow-semantics` are the similar."""
from __future__ import annotations

import logging
from typing import Any, Sequence

import tomllib

TomlType = dict[str, Any]  # type: ignore[misc]


def _get_dev_dependencies(toml_obj: TomlType) -> Sequence[str]:
    return toml_obj["tool"]["hatch"]["envs"]["dev-env"]["dependencies"]


def check_dev_dependencies() -> None:  # noqa: D103
    with open("pyproject.toml", "rb") as fp:
        metricflow_pyproject_toml = tomllib.load(fp)

    with open("metricflow-semantics/pyproject.toml", "rb") as fp:
        metricflow_semantics_pyproject_toml = tomllib.load(fp)
    metricflow_dev_env_dependencies = set(_get_dev_dependencies(metricflow_pyproject_toml))
    metricflow_semantics_dev_env_dependencies = set(_get_dev_dependencies(metricflow_semantics_pyproject_toml))

    if not metricflow_dev_env_dependencies.issuperset(metricflow_semantics_dev_env_dependencies):
        raise ValueError(
            f"Dev dependencies of `metricflow` are not a superset of those listed in `metricflow-semantics`"
            f"\n\n{metricflow_dev_env_dependencies=}"
            f"\n\n{metricflow_semantics_dev_env_dependencies=}"
        )


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s - %(levelname)s -  %(message)s", level=logging.INFO)
    check_dev_dependencies()
