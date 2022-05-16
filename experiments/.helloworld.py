from dataclasses import dataclass
from functools import cached_property
from typing import Dict, List, Literal, Optional

from cluster.base import Cluster
from cluster.local import LocalCluster
from entities.experiment import BaseExperiment
from entities.run import Run


@dataclass
class Experiment(BaseExperiment):
    @property
    def name(self) -> str:
        return "helloworld"

    @property
    def status(self) -> Literal['active', 'inactive']:
        return "active"

    @cached_property
    def clusters(self) -> Dict[str, Optional[Cluster]]:
        return {"local": LocalCluster(processes=2, cores=4, memory='2GB')}

    @property
    def num_jobs(self) -> Dict[str, int]:
        return {"local": 2}

    @property
    def runs(self) -> List[Run]:
        return [
            Run("helloworld", self.name, cmd=["echo", "Hello world!"], cluster="local"),
            Run("helloworld2", self.name, cmd=["sleep", "10"], cluster="local"),
        ]
