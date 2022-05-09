from dataclasses import dataclass
from typing import Dict, List, Literal, Optional

from dask_jobqueue import JobQueueCluster

from entities.experiment import BaseExperiment
from entities.run import Run


@dataclass
class Experiment(BaseExperiment):

    @property
    def status(self) -> Literal['active', 'inactive']:
        return "active"

    @property
    def name(self) -> str:
        return "HelloWorld"

    @property
    def clusters(self) -> Dict[str, Optional[JobQueueCluster]]:
        return {"local": None}

    @property
    def num_jobs(self) -> Dict[str, int]:
        return {"jureca-cpu": 1}

    @property
    def runs(self) -> List[Run]:
        return [Run(uid="helloworld",
                    experiment_name=self.name,
                    cluster="local",
                    cmd=["echo", "Hello World!"])]
