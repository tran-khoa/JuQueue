from abc import ABC, abstractmethod


class Cluster(ABC):
    @property
    @abstractmethod
    def processes(self) -> int:
        pass
