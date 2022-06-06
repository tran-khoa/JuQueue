from dask_jobqueue import JobQueueCluster
from dask_jobqueue.local import (LocalCluster as DaskLocalCluster, LocalJob as DaskLocalJob)

from . import register_cluster_def
from .base import ClusterDef


class _LocalJob(DaskLocalJob):

    @property
    def worker_process_threads(self):
        return 1


@register_cluster_def("local")
class LocalClusterDef(ClusterDef):
    def __init__(self, *,
                 name: str,
                 max_jobs: int,
                 num_slots: int,
                 **kwargs):
        super().__init__(name=name, max_jobs=max_jobs, num_slots=num_slots, **kwargs)

    def create_instance(self) -> JobQueueCluster:
        return DaskLocalCluster(job_cls=_LocalJob,  # noqa
                                name=self.name,
                                processes=1,
                                nanny=False,
                                asynchronous=True,
                                **self._kwargs)

    @property
    def min_jobs(self):
        return 1
