from __future__ import annotations

import asyncio
import concurrent.futures
import functools
import typing
from asyncio import Event, FIRST_COMPLETED, Task
from typing import Literal, Optional, Union

from dask.distributed import Actor
from loguru import logger

from juqueue.exceptions import NodeDeathError, NodeNotReadyError
from .node_manager import NodeManager, NodeManagerInstance

if typing.TYPE_CHECKING:
    from juqueue.backend.clusters.cluster_manager import ClusterManager


class NodeManagerWrapper(NodeManager):
    cluster_manager: ClusterManager
    index: int
    heartbeat_interval: int
    instance = Union[None, asyncio.Task, Actor]

    def __init__(self,
                 cluster_manager: ClusterManager,
                 index: int,
                 heartbeat_interval: int = 30):
        self.cluster_manager = cluster_manager
        self.index = index
        self.heartbeat_interval = heartbeat_interval

        self._address = None
        self._stopped = Event()
        self._task_actor_death = asyncio.create_task(self._heartbeat_coro(), name=f"Heartbeat-NodeManager-{self.name}")

        self.instance = asyncio.create_task(self._start_coro(), name=f"Starting-NodeManager-{self.name}")

    @property
    def cluster_name(self):
        return self.cluster_manager.cluster_name

    @property
    def status(self) -> Literal["queued", "dead", "alive"]:
        if self.instance is None:
            return "dead"
        elif isinstance(self.instance, asyncio.Task):
            if not self.instance.done():
                return "queued"
            else:
                return "dead"
        else:
            self.instance: Actor
            if (self.instance._future  # noqa
                and self.instance._future.status in ("finished", "pending")  # noqa
                and not self._stopped.is_set()):
                return "alive"
            else:
                return "dead"

    @property
    def name(self):
        return f"{self.cluster_name}-{self.index}"

    @property
    def worker(self) -> Optional[str]:
        if self._address:
            return self._address

        if isinstance(self.instance, Actor):
            self._address = self.instance._address  # noqa
        return self._address

    def block_until_death(self) -> asyncio.Task:
        return self._task_actor_death

    def mark_stopped(self):
        if not self._stopped.is_set():
            self._stopped.set()
            self.cluster_manager.request_sync()

    async def request_shutdown(self):
        if self.instance is None:
            return

        if isinstance(self.instance, asyncio.Task):
            task = self.instance
            task.cancel()
            await task  # in rare conditions, self.instance could now be an Actor

        self.mark_stopped()

        self.instance = None

    async def _start_coro(self):
        logger.info(f"Starting new node instance {self.name}....")
        try:
            actor = await self.cluster_manager.dask_client.submit(NodeManagerInstance,
                                                                  name=f"NodeManager-{self.name}",
                                                                  num_slots=self.cluster_manager.num_slots,
                                                                  work_path=self.cluster_manager.work_path,
                                                                  actor=True,
                                                                  key=f"NodeManager-{self.name}",
                                                                  resources={"slots": 1})
        except (asyncio.CancelledError, concurrent.futures.CancelledError):
            logger.info(f"Cancelling creation of NodeManager {self.name}.")
            self.instance = None
        except:
            logger.exception(f"An exception occured creating NodeManager {self.name}!")
            self.instance = None
        else:
            self.instance = actor
            self.cluster_manager.notify_new_slot.set()

    async def _heartbeat_coro(self):
        while True:
            if self.instance is None or isinstance(self.instance, asyncio.Task):
                await asyncio.sleep(self.heartbeat_interval)

            if self.instance._future.status not in ("finished", "pending"):  # noqa
                self._stopped.set()

            if self._stopped.is_set():
                return

            await asyncio.wait([asyncio.sleep(self.heartbeat_interval), self._stopped.wait()],
                               return_when=asyncio.FIRST_COMPLETED)

    def __dir__(self):
        o = set(dir(type(self)))
        if isinstance(self.instance, Actor):
            o.update(dir(self.instance))
        return sorted(o)

    def __getattribute__(self, key):
        if key.startswith("_") or not hasattr(NodeManager, key):
            return super().__getattribute__(key)

        if self.status == "dead":
            self.mark_stopped()
            raise NodeDeathError()
        elif self.status == "inactive":
            raise NodeNotReadyError()
        elif not isinstance(self.instance, Actor):
            raise NodeDeathError()

        self.instance: Actor
        obj = self.instance.__getattr__(key)

        if callable(obj):
            @functools.wraps(obj)
            def func(*args, **kwargs):
                actor_future = obj(*args, **kwargs)
                timeout_task = asyncio.create_task(asyncio.sleep(30))

                async def f():
                    done, _ = await asyncio.wait([actor_future, self._task_actor_death, timeout_task],
                                                 return_when=FIRST_COMPLETED)
                    if self._task_actor_death in done:
                        self.mark_stopped()
                        raise NodeDeathError("Notified of node death")
                    elif timeout_task in done:
                        self.mark_stopped()
                        raise NodeDeathError("Request timed out, assuming node death.")

                    result: Task = done.pop()
                    if result.exception():
                        raise result.exception()

                    return result.result()

                return asyncio.create_task(f())

            return func
        return obj

    def __repr__(self):
        return f"NodeManagerWrapper(cluster={self.cluster_name}, " \
               f"index={self.index}, " \
               f"worker={self.worker}, " \
               f"status={self.status})"
