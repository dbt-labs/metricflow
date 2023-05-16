from enum import Enum
from typing import Any, List, NoReturn, Type, TypeVar


def assert_values_exhausted(value: NoReturn) -> NoReturn:
    """Helper method to allow MyPy to guarantee an exhaustive switch through an enumeration or literal.

    DO NOT MODIFY THE TYPE SIGNATURE OF THIS FUNCTION UNLESS MYPY CHANGES HOW IT HANDLES THINGS

    To use this function correctly you MUST do an exhaustive switch through ALL values, using `is` for comparison
    (doing x == SomeEnum.VALUE will not work, nor will `x in (SomeEnum.VALUE_1, SomeEnum.VALUE_2)`).

    If mypy raises an error of the form:
      `x has incompatible type SomeEnum; expected NoReturn`
    the switch is not constructed correctly. Fix your switch statement to use `is` for all comparisons.

    If mypy raises an error of the form
      `x has incompatible type Union[Literal...]` expected NoReturn`
    the switch statement is non-exhaustive, and the values listed in the error message need to be accounted for.

    See https://mypy.readthedocs.io/en/stable/literal_types.html#exhaustiveness-checks
    For an enum example, see issue:
    https://github.com/python/mypy/issues/6366#issuecomment-560369716
    """
    assert False, f"Should be unreachable, but got {value}"


T = TypeVar("T", bound="ExtendedEnum")


class ExtendedEnum(Enum):
    """Extension of standard Enum class with some extra utilities."""

    @classmethod
    def _missing_(cls: Type[T], value: Any) -> "ExtendedEnum":  # type: ignore[misc]
        """Make enums case insensitive."""
        for member in cls:
            if member.value == value.lower():
                return member
            if member.value == value.upper():
                return member

        raise ValueError(f"Invalid enum value: `{value}` in enum {cls.__name__}")

    @classmethod
    def for_name(cls: Type[T], name: str) -> T:
        """Return enum member with this name."""
        if name not in cls.__members__:
            raise KeyError(f"Unable to find name `{name}` in enum {cls.__name__}")
        return getattr(cls, name)

    @classmethod
    def list_names(cls) -> List[str]:
        """List valid names within this enum class."""
        return list(cls.__members__.keys())
