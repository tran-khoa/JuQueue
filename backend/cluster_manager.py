import math
from asyncio import AbstractEventLoop
from pathlib import Path
from typing import Optional

from loguru import logger

from dask.distributed import Client
from tornado.ioloop import IOLoop

from cluster.base import Cluster


# wip
class ClusterManager:
    cluster_name: str
    _event_loop: AbstractEventLoop
    _client: Optional[Client]
    _cluster: Optional[Cluster]

    def __init__(self, cluster_name: str, event_loop: AbstractEventLoop):
        self.cluster_name = cluster_name
        self.max_jobs = math.inf
        self._event_loop = event_loop
        self._client = None
        self._cluster = None
        self._queue = None

    async def init(self, cluster: Cluster, max_jobs: int, force_reload: bool = False):
        if force_reload and self._client:
            logger.info(f"Client '{self.cluster_name}' is being closed and reloaded...")
            self._client.close()

        if hasattr(cluster, "log_directory"):
            Path(cluster.log_directory).expanduser().mkdir(parents=True, exist_ok=True)

        logger.info(f"Setting up cluster {self.cluster_name} with maximum_jobs={max_jobs}")
        logger.info(f"Cluster {self.cluster_name} dashboard address is {cluster.dashboard_link}")
        self.max_jobs = max_jobs

        cluster.loop = IOLoop.current()
        self._client = await Client(cluster, asynchronous=True)
        #await self.rescale()
