from __future__ import annotations

import platform
import typing
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol

from loguru import logger

from juqueue.backend.nodes.utils import Slot
from juqueue.backend.nodes import Executor
from juqueue.exceptions import NoSlotsError

if typing.TYPE_CHECKING:
    from juqueue.backend.run_instance import RunInstance
    from juqueue.definitions import RunDef


class NodeManager(Protocol):
    async def get_occupancy(self) -> float:
        pass

    async def get_slots_info(self) -> List[Dict[str, Any]]:
        pass

    async def available_slots(self) -> int:
        pass

    async def queue_run(self, run_def: RunDef, num_slots: int = 1) -> str:
        pass

    async def stop_run(self, run_id: str) -> bool:
        pass


class NodeManagerInstance(NodeManager):
    """
    Runs on a single node (job), manages the subprocesses.
    """
    slots: List[Slot]
    runs: List[Optional[RunInstance]]

    def __init__(self, num_slots: int, name: str, work_path: Path):

        logger.add(
            work_path / "logs" / "worker.log",
            rotation="1 day", compression="gz",
            format="{time} {level} {extra[name]} {extra[run_id]} {message}"
        )
        self.name = name
        self.node = platform.node()
        self.num_slots = num_slots
        self.work_path = work_path

        self.slots = [Slot(idx) for idx in range(num_slots)]

        logger.bind(name=name, run_id="@").info(f"Started NodeManager on {self.node} with {num_slots} slots.")

    async def get_slots_info(self) -> List[Dict[str, Any]]:
        return [slot.info() for slot in self.slots]

    async def available_slots(self) -> int:
        return sum(1 for slot in self.slots if not slot.is_occupied)

    async def get_occupancy(self) -> float:
        return 1 - (await self.available_slots() / self.num_slots)

    async def queue_run(self, run_def: RunDef, num_slots: int = 1) -> str:
        try:
            local_logger = logger.bind(name=self.name, run_id=run_def.global_id)

            if num_slots != 1:
                raise NotImplementedError()

            key = None
            for slot in self.slots:
                if not slot.is_occupied:
                    executor = Executor.from_def(run_def.executor, work_dir=self.work_path)
                    key = slot.assign(run_def, executor)

                    local_logger.info(f"Succesfully queued task: {slot}")
                    break

            if not key:
                raise NoSlotsError()

            return key
        except Exception as ex:
            logger.opt(exception=ex).error("Exception in queue_run")
            raise

    async def stop_run(self, run_id: str) -> bool:
        try:
            success = False
            for slot in self.slots:
                if slot.occupant == run_id:
                    await slot.cancel()
                    success = True
            return success
        except Exception as ex:
            logger.opt(exception=ex).error("Exception in queue_run")
            raise
