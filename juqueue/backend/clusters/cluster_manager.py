from __future__ import annotations

import asyncio
import contextlib
import math
import typing
from asyncio import PriorityQueue
from enum import Enum
from functools import total_ordering
from pathlib import Path
from typing import Dict, Literal, Optional, Tuple

import dask.distributed
from dask.distributed import Client, SchedulerPlugin, Scheduler, Queue
from dask_jobqueue import JobQueueCluster
from loguru import logger

from juqueue.backend.clusters.run_schedule import RunSchedule
from juqueue.backend.nodes import NodeManagerWrapper
from juqueue.backend.run_instance import RunInstance

from juqueue.definitions.cluster import ClusterDef
from juqueue.backend.utils import CancellationReason
from juqueue.exceptions import NoSlotsError, NodeDeathError, NodeNotReadyError
from juqueue.backend.clusters.utils import ExecutionResult

if typing.TYPE_CHECKING:
    from juqueue.backend.backend import Backend
    from juqueue.definitions import RunDef


@total_ordering
class NodeRemovability(Enum):
    NONE = -math.inf
    RUNNING = 0
    QUEUED = 1

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        raise NotImplementedError()


class CallbackPlugin(SchedulerPlugin):

    def __init__(self, cluster_name: str):
        super(CallbackPlugin, self).__init__()
        self.cluster_name = cluster_name

    async def remove_worker(self, scheduler: Scheduler, worker: str):
        await Queue(f"event_{self.cluster_name}").put(
            ("remove", worker)
        )


class ClusterManager:
    cluster_name: str
    _client: Optional[Client]
    _cluster: Optional[JobQueueCluster]
    _cluster_def: Optional[ClusterDef]

    _run_schedules: Dict[str, RunSchedule]

    _nodes: Dict[int, NodeManagerWrapper]

    def __init__(self, cluster_name: str, backend: Backend):
        self.cluster_name = cluster_name
        self._client = None
        self._cluster = None
        self._cluster_def = None

        self._backend = backend

        self._run_schedules = {}
        self._run_queue = PriorityQueue()

        self._num_node_requested = 0

        self._nodes = {}
        self._next_node_idx = 0

        self._dask_event_handler = None
        self._scheduler = asyncio.create_task(self._scheduler_loop(),
                                              name=f"scheduler_{cluster_name}")
        self.notify_new_slot = asyncio.Event()

        self._callback_plugin = CallbackPlugin(self.cluster_name)

        self._scheduler_lock = asyncio.Lock()
        self._stopping = False

    @property
    def num_nodes_requested(self):
        return self._num_node_requested

    @property
    def nodes(self):
        return self._nodes

    @property
    def num_slots(self):
        return self._cluster_def.num_slots

    @property
    def dask_client(self) -> Client:
        return self._client

    async def load_cluster_def(self, cluster_def: ClusterDef, force_reload: bool = False) -> Literal["unchanged",
                                                                                                     "no_update",
                                                                                                     "updated"]:
        if self._cluster_def:
            if cluster_def == self._cluster_def:
                return "unchanged"

            if self._cluster_def.is_updatable(cluster_def):
                logger.info(f"Cluster {self.cluster_name} now has maximum_jobs={cluster_def.max_jobs}")
                self._cluster_def = cluster_def
                async with self._scheduler_lock:
                    await self._rescale()
                return "updated"

            if not force_reload:
                logger.info(f"Cluster{self.cluster_name} already exists, will not update.")
                return "no_update"

        async with self._scheduler_lock:
            if self._cluster_def and force_reload:
                logger.info(f"Cluster '{self.cluster_name}' is being closed and reloaded...")
                # TODO cancel runs gracefully first!
                self._client.close()
                self._cluster.close()

            self._cluster_def = cluster_def
            self._cluster = cluster_def.create_instance()
            if hasattr(self._cluster, "log_directory"):
                Path(self._cluster.log_directory).expanduser().mkdir(parents=True, exist_ok=True)

            self._client = await Client(self._cluster, asynchronous=True)
            await self._client.register_scheduler_plugin(self._callback_plugin)

            if self._dask_event_handler is not None:
                self._dask_event_handler.cancel()

            self._dask_event_handler = asyncio.create_task(
                self._dask_event_handler_loop(), name=f"dask_event_handler_{self.cluster_name}"
            )

            await self._rescale()

            logger.info(f"Cluster {self.cluster_name} set up successfully.")

            return "updated"

    async def add_run(self, run: RunInstance):
        async with self._scheduler_lock:
            if run.global_id in self._run_schedules:
                raise ValueError("Queuing a run that is already in the queue!")

            await self._add_run(run)

    async def cancel_run(self, run_id: str, force: bool = False) -> bool:
        async with self._scheduler_lock:
            if run_id not in self._run_schedules:
                raise ValueError(f"Run {run_id} not registered.")

            run = self._run_schedules[run_id].run_instance

            if run.status == "running":
                if force:
                    await self._stop_run(run.global_id, CancellationReason.USER_CANCEL)
                else:
                    return False

            await self._remove_run(run_id)

            run.set_stopped("inactive")
            return True

    async def update_run(self, run: RunInstance, run_def: RunDef):
        async with self._scheduler_lock:
            if run.status == "running":
                await self._stop_run(run.global_id, CancellationReason.USER_CANCEL)
                await self._run_queue.put(self._run_schedules[run.global_id])

            run.run_def = run_def

    async def _stop_run(self, run_id: str, reason: CancellationReason):
        run = self._run_schedules[run_id].run_instance
        if run.status == "active":
            await run.node.stop_run(run_id, reason)
            await run.watcher

    async def rescale(self) -> Tuple[int, int]:
        async with self._scheduler_lock:
            return await self._rescale()

    async def _rescale(self) -> Tuple[int, int]:
        max_jobs = self._cluster_def.max_jobs
        current_jobs = len(self._client.scheduler_info()['workers'])

        remaining_runs = len(self._run_schedules)

        recommended_jobs = self._cluster_def.scaling_policy(remaining_runs, self._cluster_def.num_slots)
        recommended_jobs = min(recommended_jobs, max_jobs)

        if current_jobs != recommended_jobs:
            logger.info(f"Rescaling {self.cluster_name}(max_jobs={max_jobs}) "
                        f"from {current_jobs} to {recommended_jobs} jobs.")
            self._num_node_requested = recommended_jobs
            await self._sync()

        return current_jobs, recommended_jobs

    async def stop(self):
        logger.info(f"Shutting down cluster {self.cluster_name}...")
        self._stopping = True

        try:
            await asyncio.wait_for(self._scheduler_lock.acquire(), timeout=5)
        except TimeoutError:
            logger.error("Timed out waiting for scheduler lock.")

        self._scheduler.cancel()
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._scheduler, timeout=3)

        stop_tasks = [asyncio.create_task(node.shutdown()) for node in self._nodes.values()]
        if stop_tasks:
            _, failed = await asyncio.wait(stop_tasks, timeout=5)
            if failed:
                logger.error(f"Nodes {failed} could not be shut down gracefully!")

        await self._cluster.scale(0)
        await self._client.close(timeout=5)
        logger.info(f"Cluster {self.cluster_name} shut down.")

    def _add_node(self):
        idx = self._next_node_idx
        self._next_node_idx += 1

        self._nodes[idx] = NodeManagerWrapper(self, index=idx)

    async def _remove_node(self, idx: int):
        if idx not in self._nodes:
            logger.error(f"Cluster {self.cluster_name} has no node {idx}!")

        try:
            await asyncio.wait_for(self._nodes[idx].shutdown(), timeout=5)
        except asyncio.TimeoutError:
            logger.warning(f"Timed out waiting for actor {idx} to stop gracefully.")
        finally:
            del self._nodes[idx]

    def _next_removable_node(self) -> Tuple[int, NodeRemovability]:
        candidate = None
        candidate_removability = NodeRemovability.NONE

        for idx, node in self._nodes.items():
            if node.status == "dead":
                return idx, NodeRemovability.NONE
            elif node.status == "queued":
                if NodeRemovability.QUEUED > candidate_removability:
                    candidate = idx
                    candidate_removability = NodeRemovability.QUEUED
            elif node.status == "alive":
                if NodeRemovability.RUNNING > candidate_removability:
                    candidate = idx
                    candidate_removability = NodeRemovability.RUNNING
            else:
                raise RuntimeError()
        return candidate, candidate_removability

    async def _sync(self):
        """ Syncs actual running jobs to current state """

        # Check for dead actors
        self._nodes = {idx: node for idx, node in self._nodes.items() if node.status != "dead"}

        # Update to current requested number of actors
        while len(self._nodes) < self._num_node_requested:
            self._add_node()

        while len(self._nodes) > self._num_node_requested:
            idx, removability = self._next_removable_node()
            logger.info(f"Removing actor {idx} with removability {removability}...")
            await self._remove_node(idx)
            assert idx not in self._nodes

        await self._cluster.scale(jobs=self._num_node_requested)

    async def _add_run(self, run: RunInstance):
        self._run_schedules[run.global_id] = RunSchedule(run)
        await self._run_queue.put(self._run_schedules[run.global_id])

    async def _remove_run(self, run_id: str):
        """
        Removes a run from the cluster manager and triggers cluster rescaling.
        Does not cancel running runs.
        """
        if run_id not in self._run_schedules:
            raise ValueError(f"Run {run_id} not registered.")

        self._run_schedules[run_id].invalidate()
        del self._run_schedules[run_id]

    async def _schedule_to(self) -> NodeManagerWrapper:
        current_cand, current_avail = None, math.inf

        for node_idx, node in self._nodes.items():
            try:
                avail = await node.available_slots()
            except (NodeDeathError, NodeNotReadyError):
                continue

            if not avail:
                continue
            if avail == 1:
                return node
            if avail < current_avail:
                current_cand, current_avail = node, avail

        if current_cand is None:
            raise NoSlotsError()

        return current_cand

    async def _scheduler_loop(self):
        try:
            while True:
                item: RunSchedule = await self._run_queue.get()

                if not item.valid:
                    continue

                pause_scheduler = False
                async with self._scheduler_lock:
                    await self._sync()

                    try:
                        node = await self._schedule_to()
                    except NoSlotsError:
                        logger.debug(f"All workers are busy, pausing scheduler...")
                        pause_scheduler = True
                    else:
                        try:
                            key = await node.queue_run(item.run_def)
                        except NoSlotsError:
                            logger.warning(f"Node {node} unexpectedly has no available slots!")
                        except:
                            logger.exception("Exception")
                            raise
                        else:
                            logger.info(f"Node {node} accepted {item.run_def}.")
                            watcher_task = asyncio.create_task(
                                self._watcher_coro(node, item.run_instance, key),
                                name=f"watcher_{item.global_id}"
                            )
                            item.run_instance.set_running(watcher_task, node)
                        self.notify_new_slot.clear()

                if pause_scheduler:
                    await self._run_queue.put(item)

                    timeout_task = asyncio.create_task(asyncio.sleep(30))
                    notif_task = asyncio.create_task(self.notify_new_slot.wait())
                    done, _ = await asyncio.wait([timeout_task, notif_task], return_when=asyncio.FIRST_COMPLETED)
                    if notif_task in done:
                        logger.debug("Resuming scheduler as notify_new_slot set...")
                        self.notify_new_slot.clear()
                    else:
                        logger.debug("Resuming scheduler after 30 seconds...")
        except asyncio.CancelledError:
            logger.debug(f"Shutting down scheduler {self.cluster_name}.")
        except:
            logger.exception("Scheduler failure! Shutting down JuQueue!")
            await self._backend.stop()

    async def _watcher_coro(self, node: NodeManagerWrapper, run: RunInstance, key: str):
        logger.add(run.run_def.log_path / "juqueue.log",
                   filter=lambda r: r.get("run_id", None) == run.global_id)

        local_logger = logger.bind(run_id=run.global_id)
        local_logger.debug(f"Watcher for {run}@{node} has been instantiated with key {key}.")

        requeue = False

        sub = dask.distributed.Sub(key)

        try:
            while True:
                done, _ = await asyncio.wait([sub.get(timeout=5 * 60), node.block_until_death()],
                                             return_when=asyncio.FIRST_COMPLETED)

                if len(done) == 1 and node.block_until_death() in done:
                    local_logger.debug(f"Run {run.run_def} returned by {node} due to death.")
                    run.set_stopped("ready")
                    requeue = True
                    break

                res = ExecutionResult.unpack(done.pop().result())

                if res.status == "running":
                    pass
                elif res.status == "success":
                    local_logger.info(f"{run.run_def} successfully completed.")
                    run.set_stopped("finished")
                    break
                elif res.status == "cancelled":
                    if res.reason == CancellationReason.WORKER_SHUTDOWN:
                        local_logger.info(f"Run {run.run_def} returned by {node} due to shutdown.")
                        run.set_stopped("ready")
                        requeue = True
                    elif res.reason == CancellationReason.USER_CANCEL:
                        local_logger.info(f"Run {run.run_def} has been cancelled.")
                        run.set_stopped("inactive")
                    elif res.reason == CancellationReason.SERVER_SHUTDOWN:
                        run.set_stopped("ready")
                    else:
                        raise RuntimeError()
                    break
                elif res.status == "failed":
                    if isinstance(res.reason, Exception):
                        local_logger.warning(f"{run.run_def} has failed with exception {res.reason}.")
                    else:
                        local_logger.warning(f"{run.run_def} has failed with return code {res.reason}.")
                    run.set_stopped("failed")
                else:
                    raise RuntimeError(f"Invalid result status {res.status}!")
        except asyncio.CancelledError:
            # Occurs only on cancellation on server-side
            local_logger.info(f"Watcher for {run} has been cancelled by the server, assuming shutdown...")

            run.set_stopped("ready")
            requeue = True
        except asyncio.TimeoutError:
            logger.warning(f"No heartbeat detected from {key}, assuming task is dead.")

            await node.stop_run(run.global_id, CancellationReason.WORKER_SHUTDOWN)
            run.set_stopped("ready")
            requeue = True
        except:
            logger.exception(f"Exception in watcher task for {key}.")
            run.set_stopped("failed")
            raise

        if requeue:
            await self._run_queue.put(self._run_schedules[run.global_id])
        else:
            await self._remove_run(run.global_id)

        await self._rescale()
        self.notify_new_slot.set()

    async def _dask_event_handler_loop(self):
        try:
            queue = await Queue(f"event_{self.cluster_name}")
            while True:
                event = await queue.get()

                event_type, args = event
                if event_type == "remove":
                    for node_idx, node in self._nodes.items():
                        if node.worker == args:
                            logger.warning(f"Detected death of actor {node_idx}!")
                            node.mark_stopped()
                else:
                    logger.warning(f"Ignoring unknown scheduler event {event_type}")
        except asyncio.CancelledError:
            pass
        except Exception:
            if self._stopping:
                return

            logger.exception("An exception occured in the dask scheduler event handler, "
                             "exiting!")
            await self._backend.stop()
