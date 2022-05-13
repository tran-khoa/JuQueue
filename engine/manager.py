import asyncio
import datetime
import importlib
import logging
import threading
from pathlib import Path
from threading import Lock
from typing import Dict, List, Literal, Optional, Union

from dask.distributed import Client, Future, Sub

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
        self.__heartbeat_listeners: Dict[str, threading.Thread] = {}

        self.__lock = Lock()

    @property
    def runs(self) -> List[Run]:
        return list(self._loaded_runs.values())

    @property
    def future_states(self) -> Dict[str, Literal["pending", "cancelled", "lost", "error", "finished"]]:
        return {
            run_id: fut.status for run_id, fut in self._futures.items()
        }

    def run_by_id(self, run_id: str) -> Run:
        return self._loaded_runs[run_id]

    def load_experiment(self, experiment: BaseExperiment):
        self.__lock.acquire()
        self._experiment = experiment

        if self._experiment.status == "finished":
            logging.info(f"Experiment {self._experiment.name} already finished.")
            return

        for name, cluster in experiment.clusters.items():
            if cluster is not None:
                cluster.adapt(maximum_jobs=experiment.num_jobs[name])

        self.init_clusters(experiment)

        # TODO allow force-reinit of cluster, stopping all running experiments
        # TODO runs that have not been scheduled yet should be updateable, lock should stop queueing new runs
        # TODO allow run control, i.e. start/stopping all runs

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
                    ids_updated.add(run)
                    self._update_run(run)

                ids_deleted.remove(run.run_id)
            else:
                logging.info(f"Found new run {run}...")
                ids_new.add(run)
                self._add_run(run)

        for run_id in ids_deleted:
            run = self._loaded_runs[run_id]
            logging.info(f"Run {run} removed from experiment.")
            self._delete_run(run)
        self.__lock.release()

        return {"new": ids_new, "updated": ids_updated, "deleted": ids_deleted}

    def resume_runs(self, runs: List[Run]) -> List[Run]:
        self.__lock.acquire()
        resumed_runs = []
        for run in runs:
            if run.status in ("failed", "cancelled"):
                resumed_runs.append(run)
                run.status = "pending"
                run.last_run = datetime.datetime.now()
                run.save_to_disk()

                self._futures[run.run_id] = self._submit_run(run)

        self.__lock.release()

        return resumed_runs

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
                cluster.adapt(maximum_jobs=self._experiment.num_jobs[name])
                self._clients[name] = Client(cluster)
            else:
                self._clients[name] = Client()

            # TODO reactivate somehow
            #self.__heartbeat_listeners[name] = threading.Thread(target=self._heartbeat_run,
            #                                                    kwargs={"client": self._clients[name]})
            #self.__heartbeat_listeners[name].start()

    def _on_run_ended(self, fut: Future):
        xp, run_uid = fut.key.split('@')
        if xp != self._experiment.name:
            logging.warning("on_run_ended received future from different experiment")
            return

        is_successful = (fut.status == 'finished' and fut.result() == 0)
        is_cancelled = (fut.status == 'cancelled')

        self.__lock.acquire()
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

        run.save_to_disk()
        self.__lock.release()

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
        fut = client.submit(self._experiment.executor.create(run), key=f"{self._experiment.name}@{run.run_id}")
        fut.add_done_callback(self._on_run_ended)
        return fut

    def _heartbeat_run(self, client: Client):
        for run_id in Sub(f"{self.experiment_name}_heartbeat", client=client):
            self.__lock.acquire()
            run = self.run_by_id(run_id)
            if not run:
                logging.warning(f"[{self.experiment_name}] Discarding heartbeat of unknown run {run_id}.")
                continue
            run.status = "running"
            run.last_heartbeat = datetime.datetime.now()
            run.save_to_disk()
            self.__lock.release()

    def stop(self):
        for fut in self._futures.values():
            if fut:
                fut.cancel()

        self.__lock.acquire()
        self.__lock.release()


class Manager:
    managers: Dict[str, ExperimentManager]

    def __init__(self, experiments_path: Union[Path, str]):
        self.experiments_path = Path(experiments_path)
        self.managers = {}

    def load_experiments(self):
        importlib.invalidate_caches()

        results = {}
        for file in self.experiments_path.glob("*.py"):
            if file.name.startswith(".") or file.name.startswith("_"):
                continue
            module = importlib.import_module(f"experiments.{file.stem}")
            importlib.reload(module)

            xp: BaseExperiment = module.Experiment()
            if xp.status == "active":
                if xp.name not in self.managers:
                    self.managers[xp.name] = ExperimentManager(xp.name)

                result = self.managers[xp.name].load_experiment(xp)
                results[xp.name] = result
        return results

    @property
    def experiment_names(self):
        return list(self.managers.keys())

    def get_runs(self, experiment_name: str):
        if experiment_name == ALL_EXPERIMENTS:
            return [run for manager in self.managers.values() for run in manager.runs]

        if experiment_name not in self.managers:
            return None
        return self.managers[experiment_name].runs

    def stop(self):
        for manager in self.managers.values():
            manager.stop()
