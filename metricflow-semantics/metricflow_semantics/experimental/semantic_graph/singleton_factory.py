from __future__ import annotations

import logging
import threading
from abc import ABC

logger = logging.getLogger(__name__)


class SingletonFactory(ABC):
    """Mixin that describes a class to get singleton instances.

    There are plans to migrate from the `@singleton_dataclass` decorator to a factory class as it turns out the
    performance hit from the decorator can be significant in tight loops. To reduce later migration work, new / revised
    code will create instances using the factory. However, until the migration is complete, there will be places where
    the factory is used vs. using default initializer.
    """

    _instance_lock = threading.Lock()
