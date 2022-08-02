import functools
from typing import Callable, Set, TypeVar

import click

T = TypeVar("T")


# used to dedupe beta feature warnings so we can add multiple decorators for the same
# feature without spamming users
_already_warned_beta_features: Set[str] = set()


# unfortunately we can't use `typing.ParamSpec` to properly type the decorated function
# because it is only available for Python 3.10+.
def beta_feature_warning(
    feature_name: str,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Marks a function as Beta, meaning its interface should not be expected to be stable and remain unchanged."""

    def _decorator(
        func: Callable[..., T],
    ) -> Callable[..., T]:
        @functools.wraps(func)
        def _decorated(*args, **kwargs):
            if feature_name not in _already_warned_beta_features:
                click.echo(
                    click.style(f"‼️ Warning: {feature_name} is still in Beta 🧪. ", fg="red", bold=True)
                    + "As such, you should not expect it to be 100% stable or be free of bugs. Any public CLI or Python interfaces may change without prior notice."
                    " If you find any bugs or feel like something is not behaving as it should, feel free to open an issue on the Metricflow Github repo.\n"
                )
                _already_warned_beta_features.add(feature_name)
            return func(*args, **kwargs)

        return _decorated

    return _decorator
