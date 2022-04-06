from typing import Tuple, Generic, TypeVar, List, Dict

KT = TypeVar("KT")
VT = TypeVar("VT")


class MultiDiGraph(Generic[KT, VT]):
    """Generic data structure representing a directed graph with multiple edges"""

    def __init__(self) -> None:  # noqa :D
        # first key is the "from" node
        # second key is the "to" node
        self._adjacency_dict: Dict[KT, Dict[KT, List[VT]]] = {}

    def __getitem__(self, key: Tuple[KT, KT]) -> List[VT]:
        """Get all edges between the two nodes represented by this key"""
        return self._adjacency_dict[key[0]][key[1]]

    def __contains__(self, key: Tuple[KT, KT]) -> bool:
        """Graph contains at least one edge between nodes represented by ths key"""
        return key[0] in self._adjacency_dict and key[1] in self._adjacency_dict[key[0]]

    def remove_node(self, key: KT) -> None:
        """Removes a node from the graph and all edges connected to it"""
        # remove edges starting from node
        if key in self._adjacency_dict:
            del self._adjacency_dict[key]

        # remove all edges that terminate at the removed node
        for to_dict in self._adjacency_dict.values():
            for to_key in list(to_dict.keys()):
                if to_key == key:
                    del to_dict[to_key]

    def add_edge(self, from_key: KT, to_key: KT, value: VT) -> None:
        """Add an edge from `from_key` to `to_key`"""
        if from_key not in self._adjacency_dict:
            self._adjacency_dict[from_key] = {}
        if to_key not in self._adjacency_dict[from_key]:
            self._adjacency_dict[from_key][to_key] = []
        self._adjacency_dict[from_key][to_key].append(value)

    def adj(self, from_key: KT) -> Dict[KT, List[VT]]:
        """Adjascency view"""
        if from_key not in self._adjacency_dict:
            return {}
        return self._adjacency_dict[from_key]

    def edges(self, from_key: KT) -> List[VT]:
        """All edges originating from `from_key`"""
        if from_key not in self._adjacency_dict:
            return []
        return list(t for lst in self._adjacency_dict[from_key].values() for t in lst)
