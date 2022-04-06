from abc import ABC
from typing import TypeVar

VisitorOutputT = TypeVar("VisitorOutputT")


class Visitable(ABC):
    """An object that follows the visitor pattern: https://en.wikipedia.org/wiki/Visitor_pattern

    Helps to perform processing on heterogeneous types in a structured way. This class doesn't do anything, but it's a
    place to centralize this comment :)

    If we can get something like the following to work, it would be ideal.

    def accept(visitor: VisitorT[VisitorOutputT]) -> VisitorOutputT.
       pass

    """

    pass
