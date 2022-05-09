from typing import Dict, List, Literal, Optional

from dataclasses import dataclass
from dask_jobqueue import JobQueueCluster, SLURMCluster

from entities.experiment import BaseExperiment
from entities.run import Run


@dataclass
class Experiment(BaseExperiment):
    @property
    def name(self) -> str:
        return "simclr_1layer"

    @property
    def status(self) -> Literal['active', 'inactive']:
        return "inactive"

    @property
    def clusters(self) -> Dict[str, Optional[JobQueueCluster]]:
        return {
            "jureca-cpu":
                SLURMCluster(
                    queue="dc-cpu",
                    project="jinm60",
                    cores=128,
                    memory="127G",
                    interface="ib0",
                    log_directory="~/logs/helloworld",
                    processes=1,
                    extra=[
                        "--lifetime", "1h"
                    ]
                ),
            "local": None
        }

    @property
    def num_jobs(self) -> Dict[str, int]:
        return {"jureca-cpu": 1}

    @property
    def runs(self) -> List[Run]:
        return [Run(uid="helloworld",
                    experiment=self,
                    cluster="local",
                    cmd=["echo", "Hello World!"])]
