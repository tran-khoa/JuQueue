from dask_jobqueue import SLURMCluster as DaskSLURMCluster
from dask_jobqueue.core import Job
from dask_jobqueue.slurm import SLURMJob as DaskSLURMJob


class SLURMCluster(DaskSLURMCluster):
    job_cls = SLURMJob

    def __init__(self, n_workers=0, job_cls: Job = None, loop=None, security=None, silence_logs="error", name=None,
                 asynchronous=False, dashboard_address=None, host=None, scheduler_options=None, interface=None,
                 protocol="tcp://", config_name=None, **job_kwargs):
        if "extra" not in job_kwargs:
            job_kwargs["extra"] = []
        job_kwargs["extra"].extend(["--resources", "slots=1"])

        super().__init__(n_workers, job_cls, loop, security, silence_logs, name, asynchronous, dashboard_address, host,
                         scheduler_options, interface, protocol, config_name, **job_kwargs)


class SLURMJob(DaskSLURMJob):
    @property
    def worker_process_threads(self):
        return 1
