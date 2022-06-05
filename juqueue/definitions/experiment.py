from abc import ABC, abstractmethod
from pathlib import Path
from typing import List

from juqueue import get_backend
from .run import RunDef


class ExperimentDef(ABC):

    @property
    @abstractmethod
    def name(self) -> str:
        """ Unique name of this experiment """
        raise NotImplementedError()

    @property
    @abstractmethod
    def runs(self) -> List[RunDef]:
        """ List of run definitions """
        raise NotImplementedError()

    @property
    def path(self) -> Path:
        return get_backend().work_path / Path(self.name)

    @property
    def fail_period(self) -> int:
        """If run exits after N (default: 5) seconds with non-zero status code, consider run failed"""
        return 5

    def __str__(self):
        return f"Experiment(name={self.name})"
