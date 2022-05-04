import importlib
import json
import logging
import os
import threading
from typing import Dict, Union, List
from pathlib import Path

from experiments import BaseExperiment


class MainEngine:
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

    def main_loop(self):
        pass


class ExperimentEngine:
    def __init__(self, experiment: BaseExperiment):
        self.experiment = experiment

    def load_states(self):
        with open(self.experiment.path / ".juqueue", 'rt') as f:
            states = json.load(f)
            self.experiment.load_states(states)

    def __call__(self):
        if self.experiment.status == "finished":
            logging.info(f"Experiment {self.experiment.name} already finished.")
            return

    def on_run_added(self):
        pass

    def on_run_deleted(self):
        pass

    def on_run_ended(self):
        pass


MainEngine("/home/khoa/work_fzj/JuQueue/experiments").load_experiments()