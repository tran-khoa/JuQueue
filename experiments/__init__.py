import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import List, Literal, Dict, Any

from dask_jobqueue import JobQueueCluster

from config import Config
from entities.executor import Executor
from entities.parameter import Parameter
from entities.run import Run


@dataclass
class BaseExperiment(ABC):
    status: Literal['running', 'paused', 'finished'] = 'paused'

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError()

    @property
    @abstractmethod
    def cluster(self) -> JobQueueCluster:
        """
        https://jobqueue.dask.org/en/latest/generated/dask_jobqueue.SLURMCluster.html
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def workers_per_job(self) -> int:
        raise NotImplementedError()

    @property
    @abstractmethod
    def num_jobs(self) -> int:
        raise NotImplementedError()

    @property
    @abstractmethod
    def runs(self) -> List[Run]:
        raise NotImplementedError()

    @property
    def executor(self) -> Executor:
        return Executor()

    @property
    def path(self) -> Path:
        return Config.WORK_DIR / Path(self.name)

    def __repr__(self):
        return f"Experiment(name={self.name}, status={self.status})"

    @property
    def states(self) -> Dict[str, Any]:
        return {"status": self.status}

    @property
    def __metadata_path(self) -> Path:
        return self.path / "juqueue-run.json"

    def load_from_disk(self) -> bool:
        if not self.__metadata_path.exists():
            return False

        with open(self.__metadata_path, "rt") as f:
            state = json.load(f)
        self.status = state['status']
        return True

    def save_to_disk(self):
        with open(self.__metadata_path, "wt") as f:
            json.dump({"states": self.states}, f)


@dataclass
class SweepExperiment(BaseExperiment, ABC):

    @property
    @abstractmethod
    def parameters(self) -> List[Parameter]:
        raise NotImplementedError()

    @property
    def parameter_format(self) -> Literal["argparse", "k=v"]:
        return "argparse"

    @property
    def runs(self) -> List[Run]:
        pass
