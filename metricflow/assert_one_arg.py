from __future__ import annotations


def assert_exactly_one_arg_set(**kwargs) -> None:  # type: ignore
    """Throws an assertion error if 0 or more than 1 argument is not None."""
    num_set = 0
    for value in kwargs.values():
        if value is not None:
            num_set += 1

    assert num_set == 1, f"{num_set} argument(s) set instead of 1 in arguments: {kwargs}"
