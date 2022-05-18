import asyncio
import datetime
import shutil
from asyncio import AbstractEventLoop, Lock
from pathlib import Path
from typing import Dict, List, Literal, Optional, Tuple

from dask.distributed import Client, Future
from loguru import logger
from tornado.ioloop import IOLoop


from config import Config
from entities.experiment import BaseExperiment
from entities.run import Run


class ExperimentManager:

    def __init__(self, experiment_name: str, event_loop: AbstractEventLoop):
        # noinspection PyTypeChecker
        self.experiment_name = experiment_name

        self._event_loop = event_loop
        self._experiment: BaseExperiment = None
        self._loaded_runs: Dict[str, Run] = {}
        self._futures: Dict[str, Future] = {}
        self._clients: Dict[str, Client] = {}

        self._manager_lock = Lock()

        self._heartbeat_task = self._event_loop.create_task(self._heartbeat_coro())
        self._rescale_task = self._event_loop.create_task(self._rescale_coro())

        self._closing = False

    @property
    def runs(self) -> List[Run]:
        return list(self._loaded_runs.values())

    def run_by_id(self, run_id: str) -> Run:
        return self._loaded_runs[run_id]

    async def load_experiment(self, experiment: BaseExperiment):
        async with self._manager_lock:
            self._experiment = experiment

            if self._experiment.status == "finished":
                logger.info(f"Experiment {self._experiment.name} already finished.")
                return

            await self.init_clusters(experiment)

            # TODO allow force-reinit of cluster, stopping all running experiments
            # TODO runs that have not been scheduled yet should be updateable, lock should stop queueing new runs

            # Detect and load runs
            ids_new = set()
            ids_updated = set()
            ids_deleted = set(self._loaded_runs.keys())

            for run in self._experiment.runs:
                run.load_from_disk()

                if run.run_id.startswith("@") or run.global_id.startswith("@"):
                    logger.error(f"Run IDs cannot start with @, found run {run.run_id}")
                    raise ValueError(f"Run IDs cannot start with @, found run {run.run_id}")

                if run.run_id in self._loaded_runs:
                    if run == self.run_by_id(run.run_id):
                        logger.info(f"Existing run {run} is unchanged.")
                    else:
                        logger.info(f"Updating existing run {run}...")
                        logger.warning(f"{run} has changed, this has not been implemented yet :/")
                        ids_updated.add(run.run_id)
                        # self._update_run(run)

                    ids_deleted.remove(run.run_id)
                else:
                    logger.info(f"Found new run {run}...")
                    ids_new.add(run.run_id)
                    self._add_run(run)

            for run_id in ids_deleted:
                run = self._loaded_runs[run_id]
                logger.info(f"Run {run} removed from experiment.")
                self._delete_run(run)

            for name in self._clients.keys():
                await self._rescale_cluster(name)

        return {"new": ids_new, "updated": ids_updated, "deleted": ids_deleted}

    async def init_clusters(self, experiment: Optional[BaseExperiment] = None, force_reload: bool = False):
        if experiment is None:
            experiment = self._experiment

        for name, cluster in experiment.clusters.items():
            if name in self._clients:
                if not force_reload:
                    logger.info(f"Client '{name}' already registered, ignoring...")
                    continue
                else:
                    client = self._clients[name]
                    if client:
                        logger.info(f"Client '{name}' is being closed and reloaded...")
                        client.close()

            if cluster is not None:
                if hasattr(cluster, "log_directory"):
                    Path(cluster.log_directory).expanduser().mkdir(parents=True, exist_ok=True)
                logger.info(f"Setting up cluster {name} with maximum_jobs={self._experiment.num_jobs[name]}")
                logger.info(f"Cluster {name} dashboard address is {cluster.dashboard_link}")
                cluster.loop = IOLoop.current()
                self._clients[name] = await Client(cluster, asynchronous=True)
                await self._rescale_cluster(name)
            else:
                self._clients[name] = await Client(asynchronous=True)

    async def resume_runs(self, runs: List[Run],
                          states: Optional[List[Literal["failed", "cancelled", "finished"]]] = None) -> List[Run]:
        async with self._manager_lock:
            resumed_runs = []

            if states is None:
                states = ["failed", "cancelled"]

            for run in runs:
                if run.status in states:
                    resumed_runs.append(run)
                    run.state.transition("pending")
                    run.state.last_run = datetime.datetime.now()
                    run.save_to_disk()

                    self._futures[run.run_id] = self._submit_run(run)

            if resumed_runs:
                clusters = {run.cluster for run in resumed_runs}
                for cl in clusters:
                    await self._rescale_cluster(cl)

        return resumed_runs

    async def cancel_runs(self, runs: List[Run]) -> List[Run]:
        async with self._manager_lock:
            for run in runs:
                fut = self._futures.get(run.run_id, False)
                if fut:
                    fut.cancel()
                    run.state.transition("cancelled")
                    run.save_to_disk()

        clusters = {run.cluster for run in runs}
        for cl in clusters:
            await self._rescale_cluster(cl)

        return runs

    async def rescale_clusters(self) -> Dict[str, Tuple[int, int]]:
        results = {}
        async with self._manager_lock:
            for name in self._clients.keys():
                results[name] = await self._rescale_cluster(name)

        return results

    async def reset(self):
        async with self._manager_lock:
            for fut in self._futures.values():
                if fut:
                    fut.cancel()
            for run in self._loaded_runs.values():
                run.state.reset()
                run.state.transition("cancelled")

            shutil.rmtree(self._experiment.path)
            self._experiment.path.mkdir(parents=True, exist_ok=True)

    async def _heartbeat_coro(self):
        while True:
            if list(self._clients.values()):
                client = list(self._clients.values())[0]

                try:
                    if await client.get_metadata(["heartbeat"], default=None) is not None:
                        heartbeats = await client.get_metadata(["heartbeat", self.experiment_name], default=dict())
                    else:
                        heartbeats = {}
                except Exception as ex:
                    logger.exception(str(ex), exc_info=True)
                else:
                    for key, value in heartbeats.items():
                        run = self._loaded_runs.get(key, False)
                        if run:
                            run.state.last_heartbeat = datetime.datetime.fromisoformat(value)

                for run in self._loaded_runs.values():
                    run.state.check_heartbeat()

            await asyncio.sleep(Config.HEARTBEAT_INTERVAL / 2)

    async def _rescale_coro(self):
        while True:
            await self.rescale_clusters()
            await asyncio.sleep(Config.CLUSTER_ADAPT_INTERVAL.total_seconds())

    async def _rescale_cluster(self, name: str) -> Tuple[int, int]:
        client = self._clients.get(name, False)
        if not client or not hasattr(client.cluster, "scale"):
            return -1, -1

        max_jobs = self._experiment.num_jobs[name]
        workers_per_job = client.cluster.processes
        current_jobs = len(client.scheduler_info()['workers']) / workers_per_job

        remaining_runs = sum([run.status in ('running', 'pending') for run in self._loaded_runs.values()])

        recommended_jobs = self._experiment.scaling_policy[name](remaining_runs, workers_per_job)
        recommended_jobs = min(recommended_jobs, max_jobs)

        if current_jobs != recommended_jobs:
            logger.info(f"Rescaling {name}(max_jobs={max_jobs}) from {current_jobs} to {recommended_jobs} jobs.")
            await client.cluster.scale(jobs=recommended_jobs)

        return current_jobs, recommended_jobs

    async def _handle_execution(self, fut: Future):
        _, run_uid = fut.key.split('@')
        run = self._loaded_runs[run_uid]

        logger.add(run.log_path / "juqueue.log", filter=lambda r: r.get("actor", None) == fut.key)

        # noinspection PyBroadException
        return_code = "no return"
        try:
            return_code = await fut.result()
        except Exception as e:
            logger.exception(f"Actor {fut.key} failed execution", actor=f"{fut.key}")
            run.state.last_error = str(e)
            status = "error"
        else:
            if fut.status == 'finished' and return_code == 0:
                status = "finished"
            elif fut.status == 'cancelled':
                status = 'cancelled'
            else:
                status = "error"
        finally:
            if self._closing:
                logger.info(f"Stopping actor {fut.key} due to shutdown.")
                return

        async with self._manager_lock:
            run = self._loaded_runs[run_uid]

            if status == "cancelled":
                run.state.transition("cancelled")
            elif status == "finished":
                logger.info(f"{run} finished.")
                run.state.transition("finished")
            else:
                logger.warning(f"{run} considered failed.")
                run.state.transition("failed")

                with open(run.log_path / "last_error.log", "wt") as logfile:
                    if return_code != 0:
                        exception = await fut.exception()
                        logfile.write(str(exception) + "\n")
                        traceback = await fut.traceback()
                        logfile.write("\n".join(traceback.format_tb(traceback)))
                        run.state.last_error = str(exception)
                    else:
                        run.state.last_error = str(return_code)

            run.save_to_disk()

    def _add_run(self, run: Run):
        if run.status == 'pending':
            future = self._submit_run(run)
            run.state.last_run = datetime.datetime.now()
            run.save_to_disk()
            self._futures[run.run_id] = future

        self._loaded_runs[run.run_id] = run

    def _delete_run(self, run: Run):
        logger.info(f"Cancelling {run}...")

        del self._loaded_runs[run.run_id]
        if run.run_id in self._futures:
            self._futures[run.run_id].cancel(force=True)
            del self._futures[run.run_id]

    def _update_run(self, run: Run):
        raise NotImplementedError()

    def _submit_run(self, run: Run) -> Future:
        client = self._clients[run.cluster]
        fut = client.submit(self._experiment.executor.create(run),
                            key=f"{self._experiment.name}@{run.run_id}",
                            resources={'slots': 1})

        self._event_loop.create_task(self._handle_execution(fut), name=f"handle_execution_{run.global_id}")

        return fut

    async def stop(self):
        async with self._manager_lock:
            print(f"No further tasks locking {self.experiment_name}...")
            self._closing = True

            self._heartbeat_task.cancel()
            self._rescale_task.cancel()

            for fut in self._futures.values():
                if fut:
                    fut.cancel()

            for cl in self._clients.values():
                if cl:
                    cl.close()
