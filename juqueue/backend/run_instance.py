from __future__ import annotations

import asyncio
import json
import typing
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Literal, Optional, Tuple

from loguru import logger

from juqueue.backend.nodes import NodeManagerWrapper
from juqueue.backend.utils import RunStatus
from juqueue.logger import format_record

if typing.TYPE_CHECKING:
    from juqueue.definitions import RunDef
    from juqueue.backend.experiments import ExperimentManager
    from loguru import Logger


@dataclass
class RunInstance:
    manager: ExperimentManager
    run_def: RunDef
    status: RunStatus = 'inactive'
    created_at: datetime = field(default_factory=lambda: datetime.now())

    # TODO: This part should move to SchedulerItem
    watcher: Optional[asyncio.Task] = None
    node: Optional[NodeManagerWrapper] = None

    def __post_init__(self):
        if self.run_def.is_abstract:
            raise ValueError("Cannot instantiate an abstract run definition.")
        self.run_path.mkdir(parents=True, exist_ok=True)
        self.log_path.mkdir(parents=True, exist_ok=True)

        logger.add(self.run_path / "juqueue.log",
                   format=format_record,
                   rotation="1 day", retention="5 days", compression="gz",
                   filter=lambda r: r.get("run_id", None) == self.global_id)

    @property
    def run_path(self) -> Path:
        return self.run_def.path.contextualize(
            work_dir=self.manager.config.work_dir
        )

    @property
    def log_path(self):
        return self.run_def.log_path.contextualize(
            work_dir=self.manager.config.work_dir
        )

    @property
    def metadata_path(self):
        return self.run_def.metadata_path.contextualize(
            work_dir=self.manager.config.work_dir
        )

    @property
    def global_id(self):
        return self.run_def.global_id

    @property
    def id(self):
        return self.run_def.id

    def transition(self, new_status: RunStatus) -> Tuple[RunStatus, RunStatus]:
        old_status = self.status
        self.status = new_status

        if (old_status != self.status and
                not (old_status in ('running', 'ready') and new_status in ('running', 'ready'))):
            self.save_to_disk()

        return old_status, new_status

    def is_active(self) -> bool:
        return self.status in ('running', 'ready')

    @property
    def persistable_status(self) -> Literal['ready', 'failed', 'inactive', 'finished']:
        if self.is_active():
            return "ready"
        return self.status

    def load_state_dict(self, state_dict: Dict[str, Any]):
        self.status = state_dict['status']
        if 'created_at' in state_dict:
            self.created_at = datetime.fromisoformat(state_dict['created_at'])

    def state_dict(self) -> Dict[str, Any]:
        return {
            'status': self.persistable_status,
            'created_at': self.created_at.isoformat()
        }

    def save_to_disk(self):
        self.run_path.mkdir(exist_ok=True, parents=True)

        with self.metadata_path.open('wt') as f:
            json.dump(self.state_dict(), f)

    def load_from_disk(self) -> bool:
        if not self.metadata_path.exists():
            return False

        try:
            with self.metadata_path.open('rt') as f:
                self.load_state_dict(json.load(f))
            return True
        except json.JSONDecodeError:
            logger.warning(f"Stored metadata of {self.run_def.global_id} corrupted, deleting...")
            self.metadata_path.unlink()
            return False

    def set_running(self, watcher: asyncio.Task, node: NodeManagerWrapper):
        if self.status == "running":
            raise RuntimeError(f"{self} is already running.")
        self.transition("running")
        self.watcher = watcher
        self.node = node

    def set_resuming(self):
        if self.status != "running":
            self.transition("ready")

    def set_stopped(self, new_state: Literal["ready", "inactive", "failed", "finished"]):
        self.transition(new_state)
        self.watcher = None
        self.node = None

    def __repr__(self):
        return f"Run(run_def={repr(self.run_def)}, status={self.status})"

    @property
    def logger(self) -> Logger:
        return logger.bind(run_id=self.global_id)
