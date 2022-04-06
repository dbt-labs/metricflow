import os
from typing import Optional


class EnvironmentVariable:
    """Represents an environment variable and provides a way to retrieve the associated value."""

    def __init__(self, name: str, default_value: str = None):
        """Constructor.

        Args:
            name: The name of the environment variable that this represents.
            default_value: The default value to return if this environment variable is not set.
        """
        self._name = name
        self._default_value = default_value

    def exists(self) -> bool:
        """Returns true if the this environment variable exists / is set."""
        return os.getenv(self._name) is not None

    def get(self) -> str:
        """Returns the value of this environment variable as a string.

        Returns the default value if not defined. If the environment variable is not defined and no default was
        specified, this throws an exception.
        """
        value = os.getenv(self._name)
        if value is None and self._default_value is not None:
            return self._default_value
        if value is None:
            raise RuntimeError(f"Environment variable {self._name} is not defined.")
        return value

    def get_optional(self) -> Optional[str]:
        """Returns value of env variable if it exists or None if it doesn't."""
        return os.getenv(self._name)

    def get_int(self) -> int:
        """Returns value of this environment variable as an integer.

        Returns the default value if not defined. If the environment variable is not defined and no default was
        specified, this throws an exception.
        """
        value = os.environ.get(self._name)
        if value is None and self._default_value is not None:
            try:
                return int(self._default_value)
            except ValueError as e:
                raise ValueError(
                    f"Default value for {self._name} is {self._default_value}, which is not an int."
                ) from e
        if value is None:
            raise RuntimeError(f"Environment variable {self._name} is not defined.")
        try:
            return int(value)
        except ValueError as e:
            raise ValueError(f"Environment variable {self._name} is {self._default_value}, which is not an int.") from e

    @staticmethod
    def _is_valid_bool(bool_str: str) -> bool:
        """Checks if the given string can be converted into a bool."""
        return bool_str.lower() == "true" or bool_str.lower() == "false"

    def get_bool(self) -> bool:
        """Returns the value of this environment variable as a bool.

        Returns the default value if not defined. If the environment variable is not defined and no default was
        specified, this throws an exception.
        """
        value = os.environ.get(self._name)

        if value is None:
            if self._default_value is None:
                raise RuntimeError(f"Environment variable {self._name} is not defined and has no default.")

            if not EnvironmentVariable._is_valid_bool(self._default_value):
                raise ValueError(
                    f"Default value for {self._name} is {self._default_value}, which is not true or false."
                )

            return self._default_value.lower() == "true"

        if not EnvironmentVariable._is_valid_bool(value):
            raise ValueError(f"Value for {self._name} is {value}, which is not true or false.")

        return value.lower() == "true"

    @property
    def name(self) -> str:  # noqa: D
        return self._name
