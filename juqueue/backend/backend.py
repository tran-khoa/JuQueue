from __future__ import annotations

import asyncio
import importlib
import typing
from typing import Dict, Union, Optional

import dask
import yaml
from loguru import logger

from juqueue.definitions.cluster import create_cluster_def
if typing.TYPE_CHECKING:
    from juqueue.definitions import ExperimentDef
from juqueue.utils import CLUSTERS_YAML, EXPERIMENTS_DIR
from juqueue.backend.clusters.cluster_manager import ClusterManager
from juqueue.backend.experiments.experiment_manager import ExperimentManager
from .run_instance import RunInstance
from juqueue.definitions import RunDef


class Backend:
    _instance: Optional[Backend] = None
    experiment_managers: Dict[str, ExperimentManager]
    cluster_managers: Dict[str, ClusterManager]

    @classmethod
    def instance(cls, debug: bool = False):
        if cls._instance is None:
            cls._instance = Backend(debug)
        return cls._instance

    def __init__(self, debug: bool = False):
        self.experiment_managers = {}
        self.cluster_managers = {}
        self.running = True

        self._backend_lock = asyncio.Lock()

        if debug:
            dask.config.set({"logging.distributed": "debug"})

    async def initialize(self):
        logger.info("Loading clusters...")
        await self.load_clusters()
        logger.info("Loading experiments...")
        await self.load_experiments()
        logger.info("Backend initialized.")

    async def load_clusters(self):
        if not self.running:
            return
        async with self._backend_lock:
            if not CLUSTERS_YAML.exists():
                raise FileNotFoundError(f"{CLUSTERS_YAML} does not exist!")

            with CLUSTERS_YAML.open("r") as yaml_file:
                class_defs = yaml.safe_load(yaml_file)

            for name, kwargs in class_defs.items():
                for key in ("type", "cores", "memory", "max_jobs", "num_slots"):
                    if key not in kwargs:
                        raise ValueError(f"'{key}' not defined for cluster {name}! "
                                         f"Will ignore current cluster definitions!")

            for name, kwargs in class_defs.items():
                cluster_type = kwargs["type"]
                del kwargs["type"]
                kwargs["name"] = name

                try:
                    cluster_def = create_cluster_def(cluster_type=cluster_type, **kwargs)
                    if name not in self.cluster_managers:
                        self.cluster_managers[name] = ClusterManager(name, self)
                    await self.cluster_managers[name].load_cluster_def(cluster_def)
                except:
                    logger.exception(f"Could not setup cluster {name}, ignoring...")
                    if name in self.cluster_managers:
                        del self.cluster_managers[name]

        return class_defs

    async def load_experiments(self):
        if not self.running:
            return

        async with self._backend_lock:
            importlib.invalidate_caches()

            results = {}
            for file in EXPERIMENTS_DIR.glob("*.py"):
                if file.name.startswith(".") or file.name.startswith("_"):
                    continue
                try:
                    module = importlib.import_module(f"experiments.{file.stem}")
                    importlib.reload(module)
                    xp: ExperimentDef = module.Experiment()
                except:
                    logger.exception(f"Could not instantiate experiment {file.stem}, skipping...")
                    continue

                try:
                    if xp.name not in self.experiment_managers:
                        self.experiment_managers[xp.name] = ExperimentManager(xp.name, self)

                    result = await self.experiment_managers[xp.name].load_experiment(xp)
                    results[xp.name] = result
                    logger.info(f"Loaded experiment {xp.name}.")
                except:
                    logger.exception(f"Could not load experiment {file.stem}, skipping...")
                    if xp.name in self.experiment_managers:
                        del self.experiment_managers[xp.name]
            return results

    def get_cluster_manager(self, key: Union[RunInstance, RunDef, str]) -> ClusterManager:
        if isinstance(key, RunInstance):
            key = key.run_def.cluster
        elif isinstance(key, RunDef):
            key = key.cluster

        cm = self.cluster_managers.get(key, None)
        if cm is None:
            raise ValueError(f"Could not find cluster manager for {key}!")
        return cm

    async def stop(self):
        async with self._backend_lock:
            for em in self.experiment_managers.values():
                logger.info(f"Stopping manager {em.experiment_name}")
                await em.stop()
            for cm in self.cluster_managers.values():
                await cm.stop()
            del self.experiment_managers, self.cluster_managers
            logger.info("Backend stopped.")
            self.running = False
