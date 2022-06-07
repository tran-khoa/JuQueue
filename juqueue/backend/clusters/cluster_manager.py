from __future__ import annotations

import asyncio
import contextlib
import math
import typing
from asyncio import PriorityQueue
from pathlib import Path
from typing import Dict, Literal, Optional, Tuple, Union, List

import dask.distributed
from dask.distributed import Client, Queue, Scheduler, SchedulerPlugin
from dask_jobqueue import JobQueueCluster
from loguru import logger

from juqueue.backend.clusters.run_schedule import RunSchedule
from juqueue.backend.clusters.utils import ExecutionResult
from juqueue.backend.nodes import NodeManagerWrapper
from juqueue.backend.run_instance import RunInstance
from juqueue.backend.utils import RunEvent, strict_error_handler
from juqueue.config import Config, HasConfigProperty
from juqueue.definitions.cluster import ClusterDef
from juqueue.exceptions import NoSlotsError, NodeDeathError, NodeNotReadyError

if typing.TYPE_CHECKING:
    from juqueue.backend.backend import Backend
    from juqueue.definitions import RunDef


@contextlib.asynccontextmanager
async def holds_lock(name):
    logger.debug(f"{name} holding lock...")
    yield
    logger.debug(f"{name} released lock...")


class CallbackPlugin(SchedulerPlugin):

    def __init__(self, cluster_name: str):
        super(CallbackPlugin, self).__init__()
        self.cluster_name = cluster_name

    async def remove_worker(self, scheduler: Scheduler, worker: str):
        await Queue(f"event_{self.cluster_name}").put(
            ("remove", worker)
        )


class ClusterManager(HasConfigProperty):
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
        self._scheduler.add_done_callback(strict_error_handler)

        self.notify_new_slot = asyncio.Event()

        self._callback_plugin = CallbackPlugin(self.cluster_name)

        self._scheduler_lock = asyncio.Lock()
        self._stopping = False

        self._sync_requested = False

    @property
    def config(self) -> Config:
        return self._backend.config

    @property
    def work_path(self):
        return self._backend.config.work_dir

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

    async def load_cluster_def(self, cluster_def: ClusterDef, force_reload: bool = False) -> \
            Literal["unchanged", "no_update", "updated"]:
        if self._cluster_def:
            if cluster_def == self._cluster_def:
                return "unchanged"

            if self._cluster_def.is_updatable(cluster_def):
                logger.info(f"Cluster {self.cluster_name} now has maximum_jobs={cluster_def.max_jobs}")
                self._cluster_def = cluster_def
                await self.rescale()
                return "updated"

            if not force_reload:
                logger.info(f"Cluster{self.cluster_name} already exists, will not update.")
                return "no_update"

        async with self._scheduler_lock, holds_lock("load_cluster_def"):
            if self._cluster_def and force_reload:
                logger.info(f"Cluster '{self.cluster_name}' is being closed and reloaded...")

                shutdown_tasks = [asyncio.create_task(self._remove_node(node)) for node in self._nodes.keys()]
                if shutdown_tasks:
                    with contextlib.suppress(Exception):
                        await asyncio.wait_for(asyncio.gather(*shutdown_tasks, return_exceptions=True), 5)

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
            self._dask_event_handler.add_done_callback(strict_error_handler)

            await self.rescale()

            logger.info(f"Cluster {self.cluster_name} set up successfully.")

            return "updated"

    async def add_run(self, run: RunInstance):
        async with self._scheduler_lock, holds_lock("add_run"):
            if run.global_id in self._run_schedules:
                raise ValueError("Queuing a run that is already in the queue!")

            await self._add_run(run)

    async def cancel_run(self, run_id: str, force: bool = False) -> bool:
        async with self._scheduler_lock, holds_lock("cancel_run"):
            if run_id not in self._run_schedules:
                raise ValueError(f"Run {run_id} not registered.")

            run = self._run_schedules[run_id].run_instance

            if run.status == "running":
                if force:
                    try:
                        await asyncio.wait_for(self._stop_run(run.global_id), 5)
                    except asyncio.TimeoutError:
                        logger.exception(f"Timed out waiting for {run.global_id} to stop, assuming worker death.")
                    except NodeDeathError:
                        pass
                    finally:
                        await self._handle_run_event(run, RunEvent.CANCELLED_USER)
                else:
                    return False

            run.transition("inactive")
            self._remove_run(run_id)

            return True

    async def update_run(self, run: RunInstance, run_def: RunDef):
        async with self._scheduler_lock, holds_lock("update_run"):
            if run.status == "running":
                await self._stop_run(run.global_id)
                await self._handle_run_event(run, RunEvent.CANCELLED_RUN_CHANGED)

            run.run_def = run_def

    async def _stop_run(self, run_id: str) -> bool:
        """
        Explicitly stops a run, returns control only if run is cancelled.
        This stops the watcher, the run must be handled manually.
        """
        run = self._run_schedules[run_id].run_instance
        if run.status == "running":
            run.watcher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await run.watcher

            logger.debug(f"Requesting cancellation of {run_id} on {run.node}")
            return await run.node.stop_run(run_id)

    async def rescale(self) -> Tuple[int, int]:
        min_jobs = self._cluster_def.min_jobs
        max_jobs = self._cluster_def.max_jobs
        current_jobs = len(self._client.scheduler_info()['workers'])

        remaining_runs = len(self._run_schedules)

        recommended_jobs = self._cluster_def.scaling_policy(remaining_runs, self._cluster_def.num_slots)
        recommended_jobs = min(recommended_jobs, max_jobs)
        recommended_jobs = max(recommended_jobs, min_jobs)

        if current_jobs != recommended_jobs:
            logger.info(f"Rescaling {self.cluster_name}(max_jobs={max_jobs}) "
                        f"from {current_jobs} to {recommended_jobs} jobs.")
            self._num_node_requested = recommended_jobs
            self.request_sync()

        return current_jobs, recommended_jobs

    async def stop(self):
        if self._stopping:
            return

        logger.info(f"Shutting down cluster {self.cluster_name}...")
        self._stopping = True

        try:
            await asyncio.wait_for(self._scheduler_lock.acquire(), timeout=5)
        except TimeoutError:
            logger.error("Timed out waiting for scheduler lock.")

        if self._dask_event_handler is not None:
            self._dask_event_handler.cancel()

        self._scheduler.cancel()
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._scheduler, timeout=1)

        stop_tasks = [asyncio.create_task(self._remove_node(idx)) for idx in self._nodes.keys()]
        if stop_tasks:
            _, failed = await asyncio.wait(stop_tasks, timeout=5)
            if failed:
                logger.error(f"Nodes {failed} could not be shut down gracefully!")

        await self._client.close(timeout=5)
        logger.info(f"Cluster {self.cluster_name} shut down.")

    def _add_node(self):
        idx = self._next_node_idx
        self._next_node_idx += 1

        self._nodes[idx] = NodeManagerWrapper(self, index=idx)

    async def _remove_node(self, idx: int):
        """
        Cancels all runs assigned to the given node, then removes the node.
        """
        if idx not in self._nodes:
            logger.error(f"Cluster {self.cluster_name} has no node {idx}!")

        affected_runs = self.runs_by_node(idx)
        try:
            shutdown_tasks = [asyncio.create_task(self._stop_run(run.global_id)) for run in affected_runs]
            if shutdown_tasks:
                await asyncio.wait_for(asyncio.gather(*shutdown_tasks, return_exceptions=True), 2)
        except asyncio.TimeoutError:
            logger.warning(f"Timed out waiting for actor {idx} to stop gracefully.")
        except:
            logger.exception("Exception occurend during node shutdown.")

        for run in affected_runs:
            await self._handle_run_event(run, RunEvent.CANCELLED_WORKER_SHUTDOWN)

        node = self._nodes[idx]
        del self._nodes[idx]

        await node.request_shutdown()
        if node.worker:
            try:
                await self._client.retire_workers([node.worker], close_workers=True)
            except:
                logger.exception(f"Could not remove node {node.name}.")

    def runs_by_node(self, node_idx: int) -> List[RunInstance]:
        runs = []
        for rs in self._run_schedules.values():
            run = rs.run_instance
            if run.status == "running" and run.node.index == node_idx:
                runs.append(run)
        return runs

    def request_sync(self):
        if self._sync_requested:
            return

        self._sync_requested = True
        sync_task = asyncio.create_task(self.__sync())
        sync_task.add_done_callback(strict_error_handler)

    async def __sync(self):
        """ Syncs actual running jobs to current state """
        try:
            async with self._scheduler_lock, holds_lock("__sync"):
                logger.debug(f"Synchronizing {self.cluster_name}...")

                # Check for dead nodes and make sure they are dead
                dead_nodes = {idx: node for idx, node in self._nodes.items() if node.status == "dead"}
                for dead_node in dead_nodes.values():
                    if dead_node.worker:
                        await self._cluster.scale_down([dead_node.worker])
                self._nodes = {idx: node for idx, node in self._nodes.items() if node.status != "dead"}

                # Update to current requested number of nodes
                while len(self._nodes) < self._num_node_requested:
                    self._add_node()
                    self.notify_new_slot.set()

                nodes_removed = set()
                while len(self._nodes) > self._num_node_requested:
                    idx, importance = await self._next_removable_node()
                    nodes_removed.add(idx)

                    logger.info(f"Removing node {idx} with importance {importance}...")
                    await self._remove_node(idx)
                    assert idx not in self._nodes

                await self._cluster.scale(jobs=self._num_node_requested)
        finally:
            self._sync_requested = False

    async def _next_removable_node(self) -> Tuple[int, Tuple]:
        candidate = None
        importance = (math.inf, math.inf)  # Status, Number of running tasks

        for idx, node in self._nodes.items():
            if node.status == "dead":
                return idx, (0, 0)
            elif node.status == "queued":
                node_importance = (1, 0)
                if node_importance < importance:
                    candidate = idx
                    importance = node_importance
            elif node.status == "alive":
                try:
                    node_importance = (2, await node.get_occupancy())
                except NodeDeathError:
                    return idx, (0, 0)

                if node_importance < importance:
                    candidate = idx
                    importance = node_importance
            else:
                raise RuntimeError()

        return candidate, importance  # noqa

    async def _add_run(self, run: RunInstance):
        self._run_schedules[run.global_id] = RunSchedule(run)
        await self._run_queue.put(self._run_schedules[run.global_id])

    def _remove_run(self, run_id: str):
        """
        Removes a run from the cluster manager and dequeues the run, if exists.
        Does not cancel running runs.
        """
        if run_id not in self._run_schedules:
            return

        self._run_schedules[run_id].invalidate()
        del self._run_schedules[run_id]

    async def _schedule_to(self) -> NodeManagerWrapper:
        current_cand, current_avail = None, math.inf

        dead_nodes = False
        for node_idx, node in self._nodes.items():
            try:
                avail = await node.available_slots()
            except (NodeDeathError, NodeNotReadyError):
                dead_nodes = True
                continue

            if not avail:
                continue
            if avail == 1:
                return node
            if avail < current_avail:
                current_cand, current_avail = node, avail

        if dead_nodes:
            self.request_sync()

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
                async with self._scheduler_lock, holds_lock("scheduler"):
                    try:
                        node = await self._schedule_to()
                    except NoSlotsError:
                        logger.debug(f"All workers are busy, pausing scheduler...")

                        pause_scheduler = True
                    else:
                        try:
                            key = f"run_event_{item.global_id}"
                            queue = dask.distributed.Queue(key)

                            with contextlib.suppress(Exception):
                                # Empty queue
                                await asyncio.wait_for(queue.get(batch=True), timeout=1)
                        except Exception as ex:
                            logger.opt(exception=ex).exception("Could not create queue, pausing scheduler...")
                            pause_scheduler = True
                        else:
                            try:
                                remote_key = await node.queue_run(item.run_def)
                                assert key == remote_key
                            except NoSlotsError:
                                logger.warning(f"Node {node} unexpectedly has no available slots!")
                            except NodeDeathError:
                                logger.warning(f"Node {node} has died and cannot be scheduled to.")
                            except:
                                logger.exception(f"Exception while queueing {item.run_def}.")
                                raise
                            else:
                                if node.status == "dead":
                                    logger.warning(f"Node {node} has died and cannot be scheduled to.")
                                else:
                                    logger.info(f"Node {node} accepted {item.run_def} with key {key}.")
                                    watcher_task = asyncio.create_task(
                                        self._watcher_coro(node, item.run_instance, queue),
                                        name=f"watcher_{item.global_id}"
                                    )
                                    item.run_instance.set_running(watcher_task, node)
                                    self.notify_new_slot.clear()

                if pause_scheduler:
                    await self._run_queue.put(item)

                    self._sync_requested = True
                    await self.__sync()

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

    async def _handle_run_event(self, run: RunInstance, event: RunEvent, error: Union[Exception, int, None] = None):
        if event is RunEvent.RUNNING:
            run.transition("running")
        elif event is RunEvent.SUCCESS:
            run.logger.info("Run finished.")
            run.set_stopped("finished")
            self._remove_run(run.global_id)
        elif event in [RunEvent.CANCELLED_WORKER_SHUTDOWN,
                       RunEvent.CANCELLED_SERVER_SHUTDOWN,
                       RunEvent.CANCELLED_WORKER_DEATH,
                       RunEvent.CANCELLED_RUN_CHANGED]:
            run.set_stopped("ready")

            if event in [RunEvent.CANCELLED_WORKER_SHUTDOWN,
                         RunEvent.CANCELLED_WORKER_DEATH]:
                run.logger.info("Run rescheduled due to worker shutdown/failure.")
                await self._run_queue.put(self._run_schedules[run.global_id])
            elif event == RunEvent.CANCELLED_RUN_CHANGED:
                run.logger.info("Run rescheduled since the run definition was changed.")
                await self._run_queue.put(self._run_schedules[run.global_id])
            else:
                run.logger.info("JuQueue shut down.")
        elif event is RunEvent.CANCELLED_USER:
            run.set_stopped("inactive")
            self._remove_run(run.global_id)
            run.logger.info("Run cancelled by user.")
        elif event is RunEvent.FAILED:
            run.set_stopped("failed")
            self._remove_run(run.global_id)

            if error is None:
                run.logger.error("Run failed")
            elif isinstance(error, Exception):
                run.logger.bind(exception=error).error(f"Run failed with exception {type(error)}: {error}.")
            else:
                run.logger.error(f"Run failed with status code {error}.")

        await self.rescale()
        self.notify_new_slot.set()

    async def _watcher_coro(self, node: NodeManagerWrapper, run: RunInstance, queue: dask.distributed.Queue):
        run.logger.debug(f"Watcher has been instantiated with key {queue.name}.")

        timeout = 3 * 60

        try:
            while True:
                done, _ = await asyncio.wait([queue.get(timeout), node.block_until_death()],
                                             return_when=asyncio.FIRST_COMPLETED)

                if len(done) == 1 and node.block_until_death() in done:
                    await self._handle_run_event(run, RunEvent.CANCELLED_WORKER_DEATH)
                    break

                res = ExecutionResult.unpack(done.pop().result())
                await self._handle_run_event(run, res.event, res.reason)

                if res.event != RunEvent.RUNNING:
                    break

        except asyncio.CancelledError:
            # Occurs only on cancellation on server-side
            logger.debug(f"Cancelling watcher {queue.name}...")
            return
        except asyncio.TimeoutError:
            logger.debug(f"No heartbeat detected from {queue.name}, assuming task is dead.")
            with contextlib.suppress(NodeDeathError, asyncio.TimeoutError):
                await asyncio.wait_for(node.stop_run(run.global_id), 5)

            await self._handle_run_event(run, RunEvent.CANCELLED_WORKER_DEATH)
        except:
            logger.exception(f"Exception in watcher task for {queue.name}."
                             f"Stopping JuQueue, please report this issue!")
            await self._backend.stop()

    async def _dask_event_handler_loop(self):
        try:
            queue = await Queue(f"event_{self.cluster_name}")
            while True:
                event = await queue.get()

                event_type, args = event
                if event_type == "remove":
                    for node_idx, node in self._nodes.items():
                        if node.worker == args:
                            logger.debug(f"Dask reported node {node_idx} removed from cluster {self.cluster_name}.")
                            node.mark_stopped()
                else:
                    logger.warning(f"Ignoring unknown scheduler event {event_type}")
        except asyncio.CancelledError:
            pass
