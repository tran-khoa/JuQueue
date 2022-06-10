import math

from dask_jobqueue import SLURMCluster as DaskSLURMCluster
from dask_jobqueue.core import JobQueueCluster
from dask_jobqueue.slurm import SLURMJob as DaskSLURMJob

from . import register_cluster_def
from .base import ClusterDef


class _SLURMJob(DaskSLURMJob):
    @property
    def worker_process_threads(self):
        return 1


@register_cluster_def("slurm")
class SLURMClusterDef(ClusterDef):
    def __init__(self, *,
                 name: str,
                 num_slots: int,
                 max_jobs: int = math.inf,
                 **kwargs):
        if "extra" not in kwargs:
            kwargs["extra"] = []
        kwargs["extra"].extend(["--resources", "num_actors=1"])

        super().__init__(name=name, max_jobs=max_jobs, num_slots=num_slots, **kwargs)

    def create_instance(self) -> JobQueueCluster:
        return DaskSLURMCluster(job_cls=_SLURMJob,  # noqa
                                name=self.name,
                                processes=1,
                                nanny=True,
                                asynchronous=True,
                                **self._kwargs)
