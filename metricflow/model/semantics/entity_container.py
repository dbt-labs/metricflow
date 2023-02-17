from abc import ABC, abstractmethod
from typing import Generic, List, TypeVar, Dict

from metricflow.model.objects.entity import Entity

T = TypeVar("T")


class EntityContainer(ABC, Generic[T]):  # noqa: D
    @abstractmethod
    def get(self, entity_name: str) -> T:  # noqa: D
        pass

    @abstractmethod
    def values(self) -> List[T]:  # noqa: D
        pass

    @abstractmethod
    def keys(self) -> List[str]:  # noqa: D
        pass

    @abstractmethod
    def __contains__(self, item: str) -> bool:  # noqa: D
        pass

    @abstractmethod
    def put(self, key: str, value: T) -> None:  # noqa: D
        pass


class PydanticEntityContainer(EntityContainer[Entity]):  # noqa: D
    def __init__(self, entities: List[Entity]) -> None:  # noqa: D
        self._entity_index: Dict[str, Entity] = {entity.name: entity for entity in entities}

    def get(self, entity_name: str) -> Entity:  # noqa: D
        return self._entity_index[entity_name]

    def values(self) -> List[Entity]:  # noqa: D
        return list(self._entity_index.values())

    def put(self, key: str, value: T) -> None:  # noqa: D
        raise TypeError("Cannot call put on static data source container. Data sources are fixed on init")

    def _put(self, entity: Entity) -> None:
        """Dont use this unless you mean it (ie in tests). This is supposed to be static"""
        self._entity_index[entity.name] = entity

    def keys(self) -> List[str]:  # noqa: D
        return list(self._entity_index.keys())

    def __contains__(self, item: str) -> bool:  # noqa: D
        return item in self._entity_index
