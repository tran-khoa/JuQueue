import asyncio
import importlib
from asyncio import AbstractEventLoop
from pathlib import Path
from typing import Dict, Union

import dask

from entities.experiment import BaseExperiment
from .experiment_manager import ExperimentManager


class Backend:
    _event_loop: AbstractEventLoop
    managers: Dict[str, ExperimentManager]

    def __init__(self, experiments_path: Union[Path, str], event_loop: AbstractEventLoop):
        self.experiments_path = Path(experiments_path)
        self.managers = {}
        self._event_loop = event_loop
        self._backend_lock = asyncio.Lock()

        dask.config.set({"logging.distributed": "debug"})

    async def load_experiments(self):
        async with self._backend_lock:
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
                        self.managers[xp.name] = ExperimentManager(xp.name, self._event_loop)

                    result = await self.managers[xp.name].load_experiment(xp)
                    results[xp.name] = result
            return results

    @property
    def experiment_names(self):
        return list(self.managers.keys())

    def has_experiment(self, experiment_name: str) -> bool:
        return experiment_name in self.managers

    async def stop(self):
        for manager in self.managers.values():
            print(f"Stopping manager {manager.experiment_name}")
            await manager.stop()
