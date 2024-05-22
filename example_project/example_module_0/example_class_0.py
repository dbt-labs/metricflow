from abc import abstractmethod, ABC


class ExampleClass0(ABC):

    @abstractmethod
    def example_method_0(self) -> str:
        raise NotImplementedError