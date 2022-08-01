import functools
from typing import Callable, TypeVar

import click

T = TypeVar("T")


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
            click.echo(
                click.style(f"{feature_name} is still in Beta 🧪. ", bold=True)
                + "As such, you should not expect it to be 100% stable or be free of bugs. Any public CLI or Python interfaces may change without prior notice."
                + "\n\n"
                "If you find any bugs or feel like something is not behaving as it should, feel free to open an issue on the Metricflow Github repo."
            )
            return func(*args, **kwargs)

        return _decorated

    return _decorator
