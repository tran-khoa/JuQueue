import datetime
import importlib
import logging
from typing import Dict, List, Union
from pathlib import Path
from threading import Lock

from entities.run import Run
from entities.experiment import BaseExperiment

from dask.distributed import Client, Future


class Manager:

    def __init__(self, experiments_path: Union[Path, str]):
        self.experiments_path = Path(experiments_path)
        self.managers = {}

    def load_experiments(self):
        importlib.invalidate_caches()

        for file in self.experiments_path.glob("*.py"):
            if file.name.startswith(".") or file.name.startswith("_"):
                continue
            module = importlib.import_module(f"experiments.{file.stem}")
            importlib.reload(module)

            xp: BaseExperiment = module.Experiment()
            if xp.status == "active":
                if xp.name not in self.managers:
                    self.managers[xp.name] = ExperimentManager(xp)

                self.managers[xp.name].on_load(xp)

    @property
    def experiments(self):
        return {name: manager.experiment for name, manager in self.managers.items()}

    def get_runs(self, experiment_name: str):
        if experiment_name not in self.managers:
            return None
        return self.managers[experiment_name].runs


class ExperimentManager:
    def __init__(self, experiment: BaseExperiment):
        self.experiment = experiment
        self.detected_runs = {}
        self.clients = {}

        self._init_clusters(experiment)

        self.__lock = Lock()

    @property
    def runs(self) -> List[Run]:
        return list(tup["run"] for tup in self.detected_runs.values())

    def get_run(self, uid: str) -> Run:
        return self.detected_runs[uid]['run']

    def on_load(self, experiment: BaseExperiment):
        self.__lock.acquire()
        self.experiment = experiment

        if self.experiment.status == "finished":
            logging.info(f"Experiment {self.experiment.name} already finished.")
            return

        # Detect and load runs
        deleted_runs = set(self.detected_runs.keys())
        for run in self.experiment.runs:
            run.load_from_disk()

            if run.uid in self.detected_runs:
                if run == self.get_run(run.uid):
                    logging.info(f"Existing run {run} is unchanged.")
                else:
                    logging.info(f"Updating existing run {run}...")
                    logging.warning(f"{run} has changed, this has not been implemented yet :/")
                    # self.on_run_updated(run)

                deleted_runs.remove(run.uid)
            else:
                logging.info(f"Found new run {run}...")
                self._add_run(run)

        for run in deleted_runs:
            logging.info(f"Run {run} removed from experiment.")
            self._delete_run(run)
        self.__lock.release()

    def on_run_ended(self, fut: Future):
        xp, run_uid = fut.key.split('@')
        if xp != self.experiment.name:
            logging.warning("on_run_ended received future from different experiment")
            return

        is_successful = (fut.status == 'finished' and fut.result() == 0)

        self.__lock.acquire()

        run = self.detected_runs[run_uid]['run']
        if not is_successful:
            if (datetime.datetime.now() - run.last_run).seconds < self.experiment.fail_period:
                logging.warning(f"{run} considered failed.")
                self.detected_runs[run.uid].status = "failed"
            else:
                logging.info(f"Retrying {run}...")
                fut.retry()
        else:
            logging.info(f"{run} finished.")
            run.status = "finished"
        run.save_to_disk()
        self.__lock.release()

    def _init_clusters(self, experiment):
        for name, cluster in experiment.clusters.items():
            if name in self.clients:
                continue

            if cluster is not None:
                cluster.adapt(maximum_jobs=self.experiment.num_jobs)
                self.clients[name] = Client(cluster)
            else:
                self.clients[name] = Client()

    def _add_run(self, run: Run):
        future = None

        if run.status == 'active':
            future = self.__submit_run(run)
            run.last_run = datetime.datetime.now()
            run.save_to_disk()

        self.detected_runs[run.uid] = {"run": run, "fut": future}

    def _delete_run(self, run: Run):
        self.detected_runs[run.uid]["fut"].cancel(force=True)
        del self.detected_runs[run.uid]

    def _update_run(self, run: Run):
        raise NotImplementedError()

    def __submit_run(self, run: Run) -> Future:
        client = self.clients[run.cluster]
        fut = client.submit(self.experiment.executor.create(run), key=f"{self.experiment.name}@{run.uid}")
        fut.add_done_callback(self.on_run_ended)
        return fut
