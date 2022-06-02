from __future__ import annotations

import asyncio
import contextlib
import typing
import uuid
from asyncio import Task
from dataclasses import dataclass
from typing import Optional, Dict, Any, Literal, Union, List

import dask.distributed
from loguru import logger
import pickle


if typing.TYPE_CHECKING:
    from juqueue.utils import CancellationReason
    from juqueue.definitions import RunDef


@dataclass
class ExecutionResult:
    status: Literal["success", "cancelled", "failed", "running"]
    reason: Union[None, int, CancellationReason, Exception] = None

    @classmethod
    def pack(cls,
             status: Literal["success", "cancelled", "failed", "running"],
             reason: Union[None, int, CancellationReason, Exception] = None) -> bytes:
        return pickle.dumps(cls(status, reason))

    @classmethod
    def unpack(cls, obj) -> ExecutionResult:
        return pickle.loads(obj)


@dataclass
class Slot:
    index: int
    occupant: Optional[str] = None
    run_def: Optional[RunDef] = None
    task: Optional[Task] = None
    reason: Optional[CancellationReason] = None

    @property
    def is_occupied(self):
        return self.task is not None and not self.task.done()

    def info(self) -> Dict[str, Any]:
        return {
            "index": self.index,
            "occupant": self.occupant,
            "run_def": self.run_def
        }

    def assign(self, run_def: RunDef) -> str:
        self.occupant = run_def.global_id
        self.run_def = run_def

        key = f"{self.occupant}-{str(uuid.uuid4())}"
        self.task = asyncio.create_task(self._execution_coro(key, slots=[self.index]))

        return key

    async def _heartbeat_coro(self, key: str):
        fut = dask.distributed.Pub(key)
        while True:
            fut.put(ExecutionResult.pack("running"))
            await asyncio.sleep(60)

    async def _execution_coro(self, key: str, slots: List[int]):
        fut = dask.distributed.Pub(key)
        heartbeat = asyncio.create_task(self._heartbeat_coro(key))

        try:
            return_code = await asyncio.create_task(self.run_def.executor.execute(self.run_def, slots))

            heartbeat.cancel()
            if return_code == 0:
                fut.put(ExecutionResult.pack("success"))
            else:
                fut.put(ExecutionResult.pack("failed", return_code))
        except asyncio.CancelledError:
            logger.info(f"{self} has been cancelled...")

            heartbeat.cancel()
            fut.put(ExecutionResult.pack("cancelled", self.reason))
        except Exception as ex:
            logger.exception(f"An exception occured on {self}...")

            heartbeat.cancel()
            fut.put(ExecutionResult.pack("failed", ex))
        finally:
            self._free()

    async def cancel(self, reason: CancellationReason):
        if self.task is not None:
            self.reason = reason
            self.task.cancel()
            with contextlib.suppress(Exception):
                await self.task
        await self.free()

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
        self.reason = None

    def __repr__(self):
        if self.is_occupied:
            return f"Slot(occupant={self.occupant})"
        else:
            return "Slot(free)"
