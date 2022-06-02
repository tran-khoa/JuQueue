from __future__ import annotations

import platform
import typing
from asyncio import Lock
from typing import List, Optional, Dict, Any, Protocol

from loguru import logger

from juqueue.backend.clusters.utils import Slot
from juqueue.exceptions import NoSlotsError

if typing.TYPE_CHECKING:
    from juqueue.backend.run_instance import RunInstance
    from juqueue.definitions import RunDef
from juqueue.utils import WORK_DIR, CancellationReason


class NodeManager(Protocol):
    async def get_slots_info(self) -> List[Dict[str, Any]]:
        pass

    async def available_slots(self) -> int:
        pass

    async def queue_run(self, run_def: RunDef, num_slots: int = 1) -> str:
        pass

    async def stop_run(self, run_id: str, reason: CancellationReason) -> bool:
        pass

    async def shutdown(self):
        pass


class NodeManagerInstance(NodeManager):
    """
    Runs on a single node (job), manages the subprocesses.
    """
    slots: List[Slot]
    runs: List[Optional[RunInstance]]

    def __init__(self, num_slots: int, name: str):
        logger.add(
            WORK_DIR / "logs" / "worker.log",
            rotation="1 day", compression="gz",
            format="{time} {level} {extra[name]} {extra[run_id]} {message}"
        )
        self.name = name
        self.node = platform.node()
        self.num_slots = num_slots

        self.slots = [Slot(idx) for idx in range(num_slots)]
        self.__lock_instance = None

        logger.bind(name=name, run_id="@").info(f"Started JobActor on {self.node} with {num_slots} slots.")

    @property
    def _lock(self) -> Lock:
        if self.__lock_instance is None:
            self.__lock_instance = Lock()
        return self.__lock_instance

    async def get_slots_info(self) -> List[Dict[str, Any]]:
        return [slot.info() for slot in self.slots]

    async def available_slots(self) -> int:
        return sum(1 for slot in self.slots if not slot.is_occupied)

    async def queue_run(self, run_def: RunDef, num_slots: int = 1) -> str:
        local_logger = logger.bind(name=self.name, run_id=run_def.global_id)

        if num_slots != 1:
            raise NotImplementedError()

        async with self._lock:
            key = None
            for slot in self.slots:
                if not slot.is_occupied:
                    key = slot.assign(run_def)

                    local_logger.info(f"Succesfully queued task: {slot}")
                    break

        if not key:
            raise NoSlotsError()

        return key

    async def stop_run(self, run_id: str, reason: CancellationReason) -> bool:
        success = False
        for slot in self.slots:
            if slot.occupant == run_id:
                await slot.cancel(reason)
                success = True
        return success

    async def shutdown(self):
        async with self.__lock_instance:
            for slot in self.slots:
                await slot.cancel(CancellationReason.WORKER_SHUTDOWN)
