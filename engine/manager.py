import importlib
import logging
from typing import Dict, Union
from pathlib import Path

from entities.run import Run
from experiments import BaseExperiment


class MainManager:
    experiments: Dict[str, BaseExperiment]

    def __init__(self, experiments_path: Union[Path, str]):
        self.experiments_path = Path(experiments_path)
        self.experiments = self.load_experiments()

    def load_experiments(self) -> Dict[str, BaseExperiment]:
        xp_dict = {}
        importlib.invalidate_caches()

        for file in self.experiments_path.glob("*.py"):
            if file.name.startswith(".") or file.name.startswith("_"):
                continue
            module = importlib.import_module(f"experiments.{file.stem}")
            importlib.reload(module)

            xp: BaseExperiment = module.Experiment()
            xp_dict[xp.name] = xp

        return xp_dict


class ExperimentManager:
    def __init__(self, experiment: BaseExperiment):
        self.experiment = experiment
        self.detected_runs = {}

    def __call__(self):
        pass

    def on_load(self):
        # Load experiment status from disk
        self.experiment.load_from_disk()

        if self.experiment.status == "finished":
            logging.info(f"Experiment {self.experiment.name} already finished.")
            return

        # Detect and load runs
        deleted_runs = set(self.detected_runs.keys())
        for run in self.experiment.runs:
            if run.uid in self.detected_runs:
                logging.info(f"Updating existing run {run}...")
                deleted_runs.remove(run.uid)
                self.on_run_updated(run)
            else:
                logging.info(f"Found new run {run}...")
                self.on_run_added(run)

            self.detected_runs[run.uid] = run
            run.load_from_disk()

        for run in deleted_runs:
            logging.info(f"Run {run} removed from experiment.")
            self.on_run_deleted(run)
            del self.detected_runs[run.uid]

    def on_run_updated(self, run: Run):
        pass

    def on_run_added(self, run: Run):
        pass

    def on_run_deleted(self, run: Run):
        pass

    def on_run_ended(self, run: Run):
        pass


MainManager("/home/khoa/work_fzj/JuQueue/experiments").load_experiments()
