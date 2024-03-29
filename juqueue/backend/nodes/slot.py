from __future__ import annotations

import asyncio
import contextlib

from asyncio import Task
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import dask.distributed
from loguru import logger

from juqueue import RunDef
from juqueue.backend.clusters.utils import ExecutionResult
from juqueue.backend.nodes import Executor
from juqueue.backend.utils import RunEvent
from juqueue.backend.utils import strict_error_handler


@dataclass
class Slot:
    index: int
    cuda_devices: List[int]
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
        logger.debug(f"Assigning {run_def} to slot {self.index}...")
        self.occupant = run_def.global_id
        self.run_def = run_def
        self.executor = executor

        key = f"run_event_{run_def.global_id}"
        queue = dask.distributed.Queue(key)
        self.task = asyncio.create_task(self._execution_coro(queue))

        return key

    async def _heartbeat_coro(self, queue: dask.distributed.Queue):
        try:
            while True:
                await queue.put(ExecutionResult.pack(RunEvent.RUNNING))
                await asyncio.sleep(60)
        except Exception:
            logger.exception("Exception in heartbeat task.")
            raise

    async def _execution_coro(self, queue: dask.distributed.Queue):
        try:
            heartbeat = asyncio.create_task(self._heartbeat_coro(queue))
            heartbeat.add_done_callback(strict_error_handler)
        except Exception:
            logger.exception("Fatal error!")
            raise

        try:
            return_code = await self.executor.execute(self.run_def, cuda_devices=self.cuda_devices)

            heartbeat.cancel()
            if return_code == 0:
                await queue.put(ExecutionResult.pack(RunEvent.SUCCESS))
            else:
                await queue.put(ExecutionResult.pack(RunEvent.FAILED, return_code))
        except asyncio.CancelledError:
            # Assumes cluster manager takes care of explicit cancellation
            logger.info(f"{self} has been cancelled...")

            heartbeat.cancel()
            raise
        except Exception as ex:
            logger.exception(f"An exception occured on {self}...")

            heartbeat.cancel()
            await queue.put(ExecutionResult.pack(RunEvent.FAILED, ex))
            raise
        finally:
            self._free()

    async def cancel(self):
        logger.debug(f"Cancelling slot {self.index}")
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
            except Exception:
                pass

        self._free()

    def _free(self):
        self.occupant = None
        self.run_def = None
        self.task = None
        self.executor = None
        logger.info(f"Slot {self.index} has been freed...")

    def __repr__(self):
        if self.is_occupied:
            return f"Slot(index={self.index}, occupant={self.occupant})"
        else:
            return f"Slot(index={self.index}, free)"
