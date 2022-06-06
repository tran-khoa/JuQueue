from __future__ import annotations

import asyncio
import typing
from typing import Dict, List, Optional, Union

from loguru import logger

from juqueue.backend.clusters.cluster_manager import ClusterManager
from juqueue.config import Config, HasConfigProperty

if typing.TYPE_CHECKING:
    from juqueue.backend.backend import Backend
    from juqueue.definitions import ExperimentDef, RunDef
from juqueue.backend.run_instance import RunInstance


class ExperimentManager(HasConfigProperty):

    def __init__(self, experiment_name: str, backend: Backend):
        # noinspection PyTypeChecker
        self.experiment_name = experiment_name

        self._backend: Backend = backend

        # noinspection PyTypeChecker
        self._def: ExperimentDef = None
        self._runs: Dict[str, RunInstance] = {}
        self._pending_run_updates: Dict[str, asyncio.Future] = {}

        self._lock = asyncio.Lock()

    @property
    def config(self) -> Config:
        return self._backend.config

    @property
    def current_def(self):
        return self._def

    @property
    def runs(self) -> Dict[str, RunInstance]:
        return self._runs

    def run_by_id(self, run_id: str) -> Optional[RunInstance]:
        return self._runs.get(run_id, None)

    def validate_run_def(self, run_def: RunDef) -> List[str]:
        issues = []

        if run_def.id.startswith("@"):
            issues.append(f"Invalid run_id {run_def.id}!")
        if run_def.experiment_name != self.experiment_name:
            issues.append(f"{run_def} has invalid experiment name {run_def.experiment_name}, "
                          f"must be {self.experiment_name}!")
        if not self._backend.has_cluster_manager(run_def):
            issues.append(f"{run_def} defines an unknown cluster {run_def.cluster}!")

        return issues

    async def load_experiment(self, definition: ExperimentDef):
        async with self._lock:
            self._def = definition

            # Detect and load runs
            ids_new = set()
            ids_updated = set()
            ids_deleted = set(self._runs.keys())

            affected_clusters = set()

            for run_def in self._def.runs:
                errors = self.validate_run_def(run_def)
                if errors:
                    for err in errors:
                        logger.error(err)
                    logger.error(f"Due to the errors reported above, run {run_def.id} will be ignored.")
                    continue

                if run_def.id not in self._runs:
                    logger.info(f"Found new run {run_def}...")
                    run = RunInstance(self, run_def=run_def)
                    if not run.load_from_disk():
                        run.save_to_disk()
                    ids_new.add(run_def.id)
                    affected_clusters.add(run_def.cluster)
                    await self._add_run(run)
                else:
                    run = self._runs[run_def.id]
                    if run_def == self.run_by_id(run.id).run_def:
                        logger.info(f"Existing run {run_def} is unchanged.")
                    else:
                        logger.info(f"Updating existing run {run_def}...")
                        ids_updated.add(run.id)
                        affected_clusters.add(run.run_def.cluster)
                        affected_clusters.add(run_def.cluster)
                        await self._update_run(run, run_def)

                    ids_deleted.remove(run_def.id)

            for run_id in ids_deleted:
                run = self._runs[run_id]
                logger.info(f"Removing run {run.run_def} from experiment.")
                affected_clusters.add(run.run_def.cluster)
                await self._delete_run(run)

            for cluster_name in affected_clusters:
                await self.get_cluster_manager(cluster_name).rescale()

        return {"new": ids_new, "updated": ids_updated, "deleted": ids_deleted}

    async def resume_runs(self, run_ids: List[str]):
        async with self._lock:
            for rid in run_ids:
                if rid not in run_ids:
                    raise ValueError(f"Experiment {self.experiment_name} has no run {rid}.")

            logger.info(f"Resuming runs {run_ids}...")
            cms = {}
            for rid in run_ids:
                run = self._runs[rid]
                run.set_resuming()

                cm = self.get_cluster_manager(run)
                cms[cm.cluster_name] = cm
                await cm.add_run(run)

            for cm in cms.values():
                await cm.rescale()

    async def cancel_runs(self, run_ids: List[str], force: bool = False):
        async with self._lock:
            for rid in run_ids:
                if rid not in run_ids:
                    raise ValueError(f"Experiment {self.experiment_name} has no run {rid}.")

            logger.info(f"Cancelling runs {run_ids}...")
            cms = {}
            for rid in run_ids:
                run = self._runs[rid]

                cm = self.get_cluster_manager(run)
                cms[cm.cluster_name] = cm
                await self.get_cluster_manager(run).cancel_run(run.global_id, force)

            for cm in cms.values():
                await cm.rescale()

            logger.info(f"Cancelled runs {run_ids}!")

    def get_cluster_manager(self, key: Union[RunDef, RunInstance, str]) -> ClusterManager:
        return self._backend.get_cluster_manager(key)

    async def _add_run(self, run: RunInstance):
        self._runs[run.id] = run
        if run.status == "ready":
            logger.info(f"Queueing {run} to cluster {run.run_def.cluster}...")
            await self.get_cluster_manager(run).add_run(run)

    async def _delete_run(self, run: RunInstance):
        await self.get_cluster_manager(run).cancel_run(run.global_id, force=True)
        del self._runs[run.id]

    async def _update_run(self, run: RunInstance, run_def: RunDef):
        await self.get_cluster_manager(run).update_run(run, run_def)

    async def stop(self):
        logger.info(f"ExperimentManager {self.experiment_name} shut down.")
