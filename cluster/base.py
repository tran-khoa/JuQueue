from abc import ABC, abstractmethod
from dask_jobqueue import JobQueueCluster


class Cluster(ABC, JobQueueCluster):
    @property
    @abstractmethod
    def processes(self) -> int:
        pass
