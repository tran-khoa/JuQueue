from abc import ABC, abstractmethod
from pathlib import Path
from typing import List

from juqueue import get_backend
from .run import RunDef


class ExperimentDef(ABC):

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Unique name of this experiment, must be implemented by the user.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def runs(self) -> List[RunDef]:
        """
        List of run definitions, must be implemented by the user.
        """
        raise NotImplementedError()

    @property
    def path(self) -> Path:
        """
        Experiment-specific work_path

        It is not guaranteed that the corresponding runs use this path as their working directories.
        """
        return get_backend().work_path / Path(self.name)

    def __str__(self):
        return f"Experiment(name={self.name})"
