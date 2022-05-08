import datetime
import importlib
import logging
from typing import Dict, Union
from pathlib import Path
from threading import Lock

from entities.run import Run
from experiments import BaseExperiment

from dask.distributed import Client, Future


class MainManager:
    experiments: Dict[str, BaseExperiment]

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
            if xp.name not in self.managers:
                self.managers[xp.name] = ExperimentManager(xp)

            self.managers[xp.name].on_load(xp)


class ExperimentManager:
    def __init__(self, experiment: BaseExperiment):
        self.experiment = experiment
        self.detected_runs = {}
        self.client = Client(experiment.cluster)

        self.__lock = Lock()

    def init_cluster(self):
        self.experiment.cluster.adapt(maximum_jobs=self.experiment.num_jobs)

    def on_load(self, experiment: BaseExperiment):
        self.__lock.acquire()
        self.experiment = experiment

        if self.client is None:
            self.client = Client(experiment.cluster)

        # Load experiment status from disk
        self.experiment.load_from_disk()

        if self.experiment.status == "finished":
            logging.info(f"Experiment {self.experiment.name} already finished.")
            return

        # Detect and load runs
        deleted_runs = set(self.detected_runs.keys())
        for run in self.experiment.runs:
            run.load_from_disk()

            if run.uid in self.detected_runs:
                if run == self.detected_runs[run.uid]:
                    logging.info(f"Existing run {run} is unchanged.")
                else:
                    logging.info(f"Updating existing run {run}...")
                    deleted_runs.remove(run.uid)
                    self.on_run_updated(run)
            else:
                logging.info(f"Found new run {run}...")
                self.add_run(run)

        for run in deleted_runs:
            logging.info(f"Run {run} removed from experiment.")
            self.delete_run(run)
        self.__lock.release()

    def on_run_updated(self, run: Run):
        raise NotImplementedError()

    def __submit_run(self, run: Run) -> Future:
        fut = self.client.submit(self.experiment.executor.create(run), key=f"{self.experiment.name}@{run.uid}")
        fut.add_done_callback(self.on_run_ended)
        return fut

    def add_run(self, run: Run):
        future = None

        if run.status == 'active':
            future = self.__submit_run(run)
            run.last_run = datetime.datetime.now()
            run.save_to_disk()

        self.detected_runs[run.uid] = {"run": run, "fut": future}

    def delete_run(self, run: Run):
        self.detected_runs[run.uid]["fut"].cancel(force=True)
        del self.detected_runs[run.uid]

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
