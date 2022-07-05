import subprocess

from juqueue.definitions.cluster import ClusterDef, ClusterType
from abc import ABC, abstractmethod


class ClusterInstance(ABC):
    def __new__(cls, cluster_def: ClusterDef, **kwargs):
        if cluster_def.type == ClusterType.LOCAL:
            return LocalClusterInstance(cluster_def, **kwargs)

        raise NotImplementedError(f"Cluster {cluster_def.type} not implemented yet.")

    def __init__(self, cluster_def: ClusterDef):
        self.cluster_def = cluster_def

    @property
    @abstractmethod
    def target_num_jobs(self) -> int:
        raise NotImplementedError()

    @abstractmethod
    def set_num_jobs(self, num_jobs: int) -> int:
        raise NotImplementedError()


class LocalClusterInstance(ClusterInstance):
    def __init__(self, cluster_def: ClusterDef, **kwargs):
        super().__init__(cluster_def)
        self._num_workers = 0

        self.current_workers = {}

    def __new_worker(self) -> int:
        subprocess.run()

    @property
    def target_num_jobs(self) -> int:
        return self._num_workers

    def set_num_jobs(self, num_jobs: int) -> int:
        self._num_workers = min(num_jobs, self.cluster_def.max_jobs)

        return self._num_workers
