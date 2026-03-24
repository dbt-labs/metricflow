from __future__ import annotations

from typing import Callable


def mf_function_patch_target(method: Callable) -> str:
    """Helper to get the target for a patch.

    Useful so that we don't have strings that reference classes that don't get properly updated during refactoring in
    the IDE.

    Example:
        with patch(get_patch_target(DataflowToSqlPlanConverter.convert_using_specifics)):
            ...

    Annotating with `Callable` as `FunctionType` does not seem to work as expected in `mypy`.
    """
    # noinspection PyUnresolvedReferences
    return f"{method.__module__}.{method.__qualname__}"
