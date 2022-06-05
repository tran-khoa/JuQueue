from __future__ import annotations

import asyncio
import contextlib
import uuid
from asyncio import Task
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import dask.distributed
from loguru import logger

from juqueue import RunDef
from juqueue.backend.clusters.utils import ExecutionResult
from juqueue.backend.nodes import Executor
from juqueue.backend.utils import RunEvent
from juqueue.backend.utils import generic_error_handler

@dataclass
class Slot:
    index: int
    occupant: Optional[str] = None
    run_def: Optional[RunDef] = None
    executor: Optional[Executor] = None
    task: Optional[Task] = None

    @property
    def is_occupied(self):
        return self.task is not None and not self.task.done()

    def info(self) -> Dict[str, Any]:
        return {
            "index": self.index,
            "occupant": self.occupant,
            "run_def": self.run_def
        }

    def assign(self, run_def: RunDef, executor: Executor) -> str:
        self.occupant = run_def.global_id
        self.run_def = run_def
        self.executor = executor

        key = f"{self.occupant}-{str(uuid.uuid4())}"
        self.task = asyncio.create_task(self._execution_coro(key, slots=[self.index]))

        return key

    async def _heartbeat_coro(self, key: str):
        fut = dask.distributed.Pub(key)
        while True:
            fut.put(ExecutionResult.pack(RunEvent.RUNNING))
            await asyncio.sleep(60)

    async def _execution_coro(self, key: str, slots: List[int]):
        try:
            fut = dask.distributed.Pub(key)
            heartbeat = asyncio.create_task(self._heartbeat_coro(key))
            heartbeat.add_done_callback(generic_error_handler)
        except:
            logger.exception("Fatal error!")
            raise RuntimeError()

        try:
            return_code = await asyncio.create_task(self.executor.execute(self.run_def, slots))

            heartbeat.cancel()
            if return_code == 0:
                fut.put(ExecutionResult.pack(RunEvent.SUCCESS))
            else:
                fut.put(ExecutionResult.pack(RunEvent.FAILED, return_code))
        except asyncio.CancelledError:
            # Assumes cluster manager takes care of explicit cancellation
            logger.info(f"{self} has been cancelled...")

            heartbeat.cancel()
        except Exception as ex:
            logger.exception(f"An exception occured on {self}...")

            heartbeat.cancel()
            fut.put(ExecutionResult.pack(RunEvent.FAILED, ex))
        finally:
            self._free()

    async def cancel(self):
        if self.task is not None:
            self.task.cancel()
            with contextlib.suppress(Exception):
                await self.task
        self._free()

    async def free(self):
        if self.task is not None:
            try:
                await asyncio.wait_for(self.task, timeout=1)
            except asyncio.TimeoutError:
                raise RuntimeError("Cannot free a slot with a running task")
            except:
                pass

        self._free()

    def _free(self):
        self.occupant = None
        self.run_def = None
        self.task = None
        self.executor = None

    def __repr__(self):
        if self.is_occupied:
            return f"Slot(occupant={self.occupant})"
        else:
            return "Slot(free)"
