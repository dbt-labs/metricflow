from abc import ABC, abstractmethod
from typing import Generic, TypeVar

T = TypeVar("T")


class ProtocolHint(Generic[T], ABC):
    """Add this to show inspections / hints in the IDE for whether a class properly implements a protocol.

    The type parameter T should be the protocol that is expected to be implemented.

    This is only used to help generate the inspections to improve the developer experience, but otherwise does nothing.
    This also allows developers to quickly figure out implementing classes by doing a grep.

    This is a temporary solution for inspection as Protocol enhancements are not yet fully there in the IDE.
    """

    @abstractmethod
    def _implements_protocol(self) -> T:
        """Helps show IDE inspections / hints for whether the given class properly implements a protocol.

        This method should never be called - it only serves as a place where the IDE can show a red squiggle if the
        class does not implement the protocol. Hovering over the squiggle will list the fields that are out of spec.

        The return type should be the protocol that is expected to be implemented.

        The body of this method should be "return self".
        """
        pass
