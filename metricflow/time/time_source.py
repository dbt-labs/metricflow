from abc import ABC, abstractmethod
from datetime import datetime


class TimeSource(ABC):
    """Provides time to classes that need a sense of time.

    A static time source can be used for testing, while a time source that uses the current time is used for production.
    """

    @abstractmethod
    def get_time(self) -> datetime:  # noqa: D
        pass
