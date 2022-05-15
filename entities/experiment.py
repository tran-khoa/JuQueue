from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional

from dask_jobqueue import JobQueueCluster

from config import Config
from entities.executor import Executor
from entities.run import Run
from entities.scaling import ScalingPolicy


@dataclass
class BaseExperiment(ABC):

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError()

    @property
    def status(self) -> Literal['active', 'inactive']:
        return 'active'

    @cached_property
    @abstractmethod
    def clusters(self) -> Dict[str, Optional[JobQueueCluster]]:
        """
        https://jobqueue.dask.org/en/latest/generated/dask_jobqueue.SLURMCluster.html
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def num_jobs(self) -> Dict[str, int]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def runs(self) -> List[Run]:
        raise NotImplementedError()

    @property
    def executor(self) -> Executor:
        return Executor()

    @property
    def scaling_policy(self) -> Dict[str, Callable[[int, int], int]]:
        return {cl: ScalingPolicy.maximize_running for cl in self.clusters.values()}

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

    @property
    def fail_period(self) -> int:
        """If run exits after N (default: 120) seconds with non-zero status code, consider run failed"""
        return 120
