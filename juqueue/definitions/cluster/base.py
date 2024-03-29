from abc import ABC, abstractmethod

from dask_jobqueue import JobQueueCluster
from loguru import logger

from juqueue.backend.clusters import ScalingPolicy


class ClusterDef(ABC):

    def __init__(self, *, name: str, max_jobs: int, num_slots: int, scaling_policy: str = "maximize_running",
                 cuda_devices_per_slot: int, **kwargs):
        self.name = name
        self.max_jobs = max_jobs
        self.num_slots = num_slots
        self.cuda_devices_per_slot = cuda_devices_per_slot
        self._kwargs = kwargs

        self.scaling_policy = getattr(ScalingPolicy, scaling_policy, None)
        if self.scaling_policy is None:
            logger.error(f"Invalid scaling policy {scaling_policy}! Defaulting to 'maximize_running'.")
            self.scaling_policy = ScalingPolicy.maximize_running

        if 'processes' in self._kwargs:
            logger.warning(f"Cluster {name} has key 'processes', will ignore.")
            del self._kwargs['processes']

        if "extra" not in self._kwargs:
            self._kwargs["extra"] = []
        self._kwargs["extra"].extend(["--resources num_actors=1", "--nworkers 1"])

    @abstractmethod
    def create_instance(self) -> JobQueueCluster:
        raise NotImplementedError()

    def __eq__(self, other):
        if not isinstance(other, ClusterDef):
            return False

        for p in ("name", "max_jobs", "_kwargs"):
            if getattr(self, p) != getattr(other, p):
                return False

        return True

    def is_updatable(self, other) -> bool:
        """
        Determines whether difference necessitates cluster reinstantiation -> False
        """
        if not isinstance(other, ClusterDef):
            raise ValueError("Not comparing to another ClusterDef!")

        for p in ("name", "_kwargs", "cuda_devices_per_slot"):
            if getattr(self, p) != getattr(other, p):
                return False
        return True

    @property
    def min_jobs(self):
        return 0
