from typing import List

from dataclasses import dataclass
from dask_jobqueue import JobQueueCluster, SLURMCluster

from . import BaseExperiment
from entities.run import Run


@dataclass
class Experiment(BaseExperiment):
    @property
    def name(self) -> str:
        return "HelloWorld"

    @property
    def cluster(self) -> JobQueueCluster:
        return None
        """
        return SLURMCluster(
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
        )
        """

    @property
    def workers_per_job(self) -> int:
        return 1

    @property
    def num_jobs(self) -> int:
        return 1

    @property
    def runs(self) -> List[Run]:
        return [Run(uid="helloworld",
                    experiment=self,
                    cmd=["echo", "Hello World!"])]
