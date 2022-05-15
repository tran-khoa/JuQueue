import importlib
from pathlib import Path
from typing import Dict, Union

import dask

from entities.experiment import BaseExperiment
from experiment import ALL_EXPERIMENTS, ExperimentManager


class Manager:
    managers: Dict[str, ExperimentManager]

    def __init__(self, experiments_path: Union[Path, str]):
        self.experiments_path = Path(experiments_path)
        self.managers = {}

        dask.config.set({"logging.distributed": "debug", "logging.tornado": "debug"})

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
            raise ValueError(f"Unknown experiment {experiment_name}")
        return self.managers[experiment_name].runs

    def stop(self):
        for manager in self.managers.values():
            print(f"Stopping manager {manager.experiment_name}")
            manager.stop()
