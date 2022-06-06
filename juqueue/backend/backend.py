from __future__ import annotations

import asyncio
import importlib
import importlib.util
import os
import sys
import threading
import time
import typing
from typing import Dict, Optional, Union

import dask
import yaml
from loguru import logger

from juqueue import BackendInstance, get_backend
from juqueue.config import Config, HasConfigField
from juqueue.definitions.cluster import create_cluster_def

if typing.TYPE_CHECKING:
    from juqueue.definitions import ExperimentDef
from juqueue.backend.clusters.cluster_manager import ClusterManager
from juqueue.backend.experiments.experiment_manager import ExperimentManager
from .run_instance import RunInstance
from juqueue.definitions import RunDef


class Backend(HasConfigField):
    _instance: Optional[Backend] = None
    experiment_managers: Dict[str, ExperimentManager]
    cluster_managers: Dict[str, ClusterManager]

    @classmethod
    def instance(cls) -> Backend:
        return get_backend()

    def __init__(self, config: Config, on_shutdown: typing.Coroutine):
        if BackendInstance.ready():
            raise RuntimeError("BackendInstance already exists, cannot instantiate backend multiple times.")
        BackendInstance.set(self)

        self.config = config

        self.definitions_path = config.def_dir
        self.work_path = config.work_dir

        sys.path.insert(0, str(self.definitions_path))

        self.experiment_managers = {}
        self.cluster_managers = {}
        self.running = True

        self._backend_lock = asyncio.Lock()

        if config.debug:
            dask.config.set({"logging.distributed": "debug"})

        self._on_shutdown_handler = on_shutdown

    async def initialize(self):
        try:
            logger.info("Loading clusters...")
            await self.load_clusters()
            logger.info("Loading experiments...")
            await self.load_experiments()
            logger.info("Backend initialized.")
        except:
            logger.exception("Failed backend initialization!")
            await self.stop()

    async def load_clusters(self):
        if not self.running:
            return
        async with self._backend_lock:
            yaml_path = self.definitions_path / "clusters.yaml"

            if not yaml_path.exists():
                raise FileNotFoundError(f"{yaml_path} does not exist!")

            with yaml_path.open("r") as yaml_file:
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

            experiments_path = self.definitions_path / "experiments"
            if not experiments_path.exists():
                raise FileNotFoundError(f"{experiments_path} does not exist!")

            results = {}
            for file in experiments_path.glob("*.py"):
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

    def has_cluster_manager(self, key: Union[RunInstance, RunDef, str]) -> bool:
        if isinstance(key, RunInstance):
            key = key.run_def.cluster
        elif isinstance(key, RunDef):
            key = key.cluster

        return key in self.cluster_managers

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
        self.schedule_kill()

        logger.info("Stopping backend...")
        async with self._backend_lock:
            for em in self.experiment_managers.values():
                logger.info(f"Stopping manager {em.experiment_name}")
                await em.stop()
            for cm in self.cluster_managers.values():
                await cm.stop()
            del self.experiment_managers, self.cluster_managers
            logger.info("Backend stopped.")
            self.running = False

        await self._on_shutdown_handler

    def schedule_kill(self, delay: int = 30):
        pid = os.getpid()

        def _kill():
            time.sleep(delay)
            print(f"Killing JuQueue after {delay} seconds. Goodbye.")
            os.system(f"kill -9 {pid}")

        thread = threading.Thread(target=_kill, daemon=True)
        thread.start()
        return thread
