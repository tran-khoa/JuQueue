import datetime
import logging
import shutil
import threading
from pathlib import Path
from threading import Lock
from typing import Dict, List, Literal, Optional, Tuple

from dask.distributed import Client, Future, Sub

from config import Config
from entities.experiment import BaseExperiment
from entities.run import Run

ALL_EXPERIMENTS = "@ALL_EXPERIMENTS"
ALL_RUNS = "@ALL_RUNS"


class ExperimentManager:

    def __init__(self, experiment_name: str):
        # noinspection PyTypeChecker
        self.experiment_name = experiment_name
        self._experiment: BaseExperiment = None
        self._loaded_runs: Dict[str, Run] = {}
        self._futures: Dict[str, Future] = {}
        self._clients: Dict[str, Client] = {}

        self.__lock = Lock()

        self.__heartbeat_setup = False
        self.__heartbeat_timer = None
        self.poll_heartbeats()

        self.__rescale_timer = None
        self.on_rescale_timer()

    def poll_heartbeats(self):
        if list(self._clients.values()):
            client = list(self._clients.values())[0]

            try:
                if client.get_metadata(["heartbeat"], default=None) is not None:
                    heartbeats = client.get_metadata(["heartbeat", self.experiment_name], default=dict())
                else:
                    heartbeats = {}
            except KeyError:
                pass
            else:
                for key, value in heartbeats.items():
                    run = self._loaded_runs.get(key, False)
                    if run:
                        run.last_heartbeat = datetime.datetime.fromisoformat(value)
                        run.save_to_disk()
        self.__heartbeat_timer = threading.Timer(Config.HEARTBEAT_INTERVAL / 2, self.poll_heartbeats)
        self.__heartbeat_timer.start()

    def on_rescale_timer(self):
        self.rescale_clusters()

        self.__rescale_timer = threading.Timer(Config.CLUSTER_ADAPT_INTERVAL.total_seconds(), self.on_rescale_timer)
        self.__rescale_timer.start()

    @property
    def runs(self) -> List[Run]:
        return list(self._loaded_runs.values())

    def run_by_id(self, run_id: str) -> Run:
        return self._loaded_runs[run_id]

    def load_experiment(self, experiment: BaseExperiment):
        with self.__lock:
            self._experiment = experiment

            if self._experiment.status == "finished":
                logging.info(f"Experiment {self._experiment.name} already finished.")
                return

            self.init_clusters(experiment)

            # TODO allow force-reinit of cluster, stopping all running experiments
            # TODO runs that have not been scheduled yet should be updateable, lock should stop queueing new runs

            # Detect and load runs
            ids_new = set()
            ids_updated = set()
            ids_deleted = set(self._loaded_runs.keys())

            for run in self._experiment.runs:
                run.load_from_disk()

                if run.run_id.startswith("@"):
                    logging.error(f"Run IDs cannot start with @, found run {run.run_id}")
                    raise ValueError(f"Run IDs cannot start with @, found run {run.run_id}")

                if run.run_id in self._loaded_runs:
                    if run == self.run_by_id(run.run_id):
                        logging.info(f"Existing run {run} is unchanged.")
                    else:
                        logging.info(f"Updating existing run {run}...")
                        logging.warning(f"{run} has changed, this has not been implemented yet :/")
                        ids_updated.add(run.run_id)
                        self._update_run(run)

                    ids_deleted.remove(run.run_id)
                else:
                    logging.info(f"Found new run {run}...")
                    ids_new.add(run.run_id)
                    self._add_run(run)

            for run_id in ids_deleted:
                run = self._loaded_runs[run_id]
                logging.info(f"Run {run} removed from experiment.")
                self._delete_run(run)

            for name in self._clients.keys():
                self._rescale_cluster(name)

        return {"new": ids_new, "updated": ids_updated, "deleted": ids_deleted}

    def resume_runs(self, runs: List[Run], states: Optional[List[Literal["failed", "cancelled", "finished"]]] = None) \
            -> List[Run]:
        with self.__lock:
            resumed_runs = []

            if states is None:
                states = ["failed", "cancelled"]

            for run in runs:
                if run.status in states:
                    resumed_runs.append(run)
                    run.status = "pending"
                    run.last_run = datetime.datetime.now()
                    run.save_to_disk()

                    self._futures[run.run_id] = self._submit_run(run)

        return resumed_runs

    def cancel_run(self, runs: List[Run]) -> List[Run]:
        with self.__lock:
            for run in runs:
                fut = self._futures.get(run.run_id, False)
                if fut:
                    fut.cancel()
                run.status = "cancelled"
                run.save_to_disk()

        return runs

    def init_clusters(self, experiment: Optional[BaseExperiment] = None, force_reload: bool = False):
        if experiment is None:
            experiment = self._experiment

        for name, cluster in experiment.clusters.items():
            if name in self._clients:
                if not force_reload:
                    logging.info(f"Client '{name}' already registered, ignoring...")
                    continue
                else:
                    client = self._clients[name]
                    if client:
                        logging.info(f"Client '{name}' is being closed and reloaded...")
                        client.close()

            if cluster is not None:
                if hasattr(cluster, "log_directory"):
                    Path(cluster.log_directory).expanduser().mkdir(parents=True, exist_ok=True)
                logging.info(f"Setting up cluster {name} with maximum_jobs={self._experiment.num_jobs[name]}")
                logging.info(f"Cluster {name} dashboard address is {cluster.dashboard_link}")
                self._clients[name] = Client(cluster)
                self._rescale_cluster(name)
            else:
                self._clients[name] = Client()

    def rescale_clusters(self) -> Dict[str, Tuple[int, int]]:
        results = {}
        with self.__lock:
            for name in self._clients.keys():
                results[name] = self._rescale_cluster(name)

        return results

    def _rescale_cluster(self, name: str) -> Tuple[int, int]:
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
            logging.info(f"Rescaling {name}(max_jobs={max_jobs}) from {current_jobs} to {recommended_jobs} jobs.")
            client.cluster.scale(recommended_jobs)
        return current_jobs, recommended_jobs

    def on_run_ended(self, fut: Future):
        xp, run_uid = fut.key.split('@')
        if xp != self._experiment.name:
            logging.warning("on_run_ended received future from different experiment")
            return

        is_successful = (fut.status == 'finished' and fut.result() == 0)
        is_cancelled = (fut.status == 'cancelled')

        with self.__lock:
            run = self._loaded_runs[run_uid]

            if run.status not in ("pending", "running"):
                logging.error(f"Ended {run} with status {run.status}!")
                run.status = "pending"

            if is_cancelled:
                run.status = "cancelled"
            elif is_successful:
                logging.info(f"{run} finished.")
                run.status = "finished"
            else:
                if (datetime.datetime.now() - run.last_run).seconds < self._experiment.fail_period:
                    logging.warning(f"{run} considered failed.")
                    run.status = "failed"
                else:
                    logging.info(f"Retrying {run}...")
                    fut.retry()
                run.last_error = str(fut.exception(timeout=3))

            run.save_to_disk()

    def _add_run(self, run: Run):
        if run.status == 'pending':
            future = self._submit_run(run)
            run.last_run = datetime.datetime.now()
            run.save_to_disk()
            self._futures[run.run_id] = future

        self._loaded_runs[run.run_id] = run

    def _delete_run(self, run: Run):
        logging.info(f"Cancelling {run}...")

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
                            resources={'slots': 1},
                            pure=False)
        fut.add_done_callback(self.on_run_ended)
        return fut

    def _heartbeat_run(self, client: Client):
        for run_id in Sub(f"{self.experiment_name}_heartbeat", client=client):
            with self.__lock:
                run = self.run_by_id(run_id)
                if not run:
                    logging.warning(f"[{self.experiment_name}] Discarding heartbeat of unknown run {run_id}.")
                    continue
                run.status = "running"
                run.last_heartbeat = datetime.datetime.now()
                run.save_to_disk()

    def reset(self):
        with self.__lock:
            for fut in self._futures.values():
                if fut:
                    fut.cancel()
            for run in self._loaded_runs.values():
                run.status = "cancelled"
                run.last_run = None
                run.last_heartbeat = None

            shutil.rmtree(self._experiment.path)
            self._experiment.path.mkdir(parents=True, exist_ok=True)

    def stop(self):
        print(f"Clearing lock of {self.experiment_name}...")
        self.__lock.acquire(timeout=60)
        self.__lock.release()
        print(f"Lock of {self.experiment_name} cleared.")

        if self.__heartbeat_timer:
            self.__heartbeat_timer.cancel()

        if self.__rescale_timer:
            self.__rescale_timer.cancel()

        for fut in self._futures.values():
            if fut:
                fut.cancel()

        for cl in self._clients.values():
            if cl:
                cl.close()
