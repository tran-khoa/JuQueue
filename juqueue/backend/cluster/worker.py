from __future__ import annotations

import contextlib
import sys
import typing
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Protocol

import asyncio
import click
import engineio.exceptions
import socketio
from loguru import logger

from juqueue.backend.cluster.messages import (BaseMessage,)
from juqueue.backend.entities.executor import Executor
from juqueue.exceptions import NoSlotsError

if typing.TYPE_CHECKING:
    from juqueue.backend.entities.run_instance import RunInstance


class SendsMessage(Protocol):
    async def send_message(self, msg: BaseMessage):
        ...


@dataclass
class Slot:
    index: int
    occupant: Optional[str] = None

    @property
    def is_free(self):
        return self.occupant is None

    def assign(self, occupant: str):
        if occupant is not None:
            raise ValueError("Slot is not free, cannot assign!")
        self.occupant = occupant


@dataclass
class Occupant:
    run: RunInstance
    slots: List[Slot] = field(default_factory=list)
    task: Optional[asyncio.Task] = None

    @property
    def id(self) -> str:
        return self.run.id


class ExecutionManager:
    slots: List[Slot]
    occupants: Dict[str, Occupant]

    def __init__(self, num_slots: int, sender: SendsMessage):
        self.num_slots = num_slots

        self.slots = [Slot(idx) for idx in range(num_slots)]
        self.occupants = {}

        self.lock = asyncio.Lock()
        self.sender = sender

    def free_slots(self) -> List[Slot]:
        return [slot for slot in self.slots if slot.is_free]

    @property
    def occupancy(self) -> float:
        return 1 - len(self.free_slots()) / self.num_slots

    async def shutdown(self):
        async with self.lock:
            for occupant in self.occupants.values():
                occupant.task.cancel()

            self.occupants = {}

    async def stop_run(self, run_id: str) -> bool:
        async with self.lock:
            if run_id not in self.occupants:
                return False

            self.occupants[run_id].task.cancel()
            for slot in self.occupants[run_id].slots:
                slot.occupant = None

            del self.occupants[run_id]
            return True

    async def execution_coro(self, run: RunInstance, slots: List[Slot]):
        executor = Executor.from_def(run.run_def.executor)

        try:
            return_code = await executor.execute(run, slots)

            if return_code == 0:
                await queue.put(ExecutionResult.pack(RunEvent.SUCCESS))
            else:
                await queue.put(ExecutionResult.pack(RunEvent.FAILED, return_code))
        except asyncio.CancelledError:
            # Assumes cluster manager takes care of explicit cancellation
            logger.info(f"{self} has been cancelled...")

            raise
        except Exception as ex:
            logger.exception(f"An exception occured on {self}...")

            await queue.put(ExecutionResult.pack(RunEvent.FAILED, ex))
            raise
        finally:
            self._free()

    async def queue_run(self, run: RunInstance):
        async with self.lock:
            free_slots = self.free_slots()
            if 1 > len(free_slots):  # TODO: variable number of slots
                logger.warning(f"Not enough free slots for {run}.")
                raise NoSlotsError()

            task = asyncio.create_task(self.execution_coro(run))
            slot = free_slots[0].assign(run.id)

            self.occupants[run.id] = Occupant(
                run=run,
                slots=[slot],
                task=task
            )


@dataclass
class WorkerConfig:
    cluster_name: str
    worker_id: str
    manager_addr: str
    num_slots: int
    report_interval: int
    timeout: int
    num_retries: int


class WorkerShutdownMessage:
    pass


class Worker(SendsMessage):

    def __init__(self, **kwargs):
        self.cfg = WorkerConfig(**kwargs)

        self.sio = None
        self.manager = None
        self.ready = asyncio.Event()

    async def send_message(self, msg: BaseMessage):
        await self.sio.emit("worker_message", msg.serialize())

    async def disconnect(self):
        logger.info("Worker disconnected!")

    async def start(self, wait_timeout=10, wait=True):
        self.manager = ExecutionManager(num_slots=self.cfg.num_slots, sender=self)

        self.sio = socketio.AsyncClient(
            reconnection_attempts=self.cfg.num_retries,
            reconnection_delay=self.cfg.timeout,
            request_timeout=self.cfg.timeout
        )
        self.sio.on("disconnect", self.disconnect)

        try:
            await asyncio.ensure_future(self.sio.connect(self.cfg.manager_addr,
                                                         wait_timeout=wait_timeout,
                                                         namespaces=["/"]))
            logger.info(f"Successfully connected to {self.cfg.manager_addr}!")
            self.ready.set()
            if wait:
                await self.sio.wait()
        except socketio.exceptions.ConnectionError as ex:
            raise ConnectionError(f"Could not connect to {self.cfg.manager_addr}!") from ex
        except asyncio.CancelledError:
            ...
        finally:
            with contextlib.suppress(Exception):
                await self.sio.emit(WorkerShutdownMessage())
            await self.sio.disconnect()
            logger.info("Worker shut down!")


async def worker(**kwargs):
    w = Worker(**kwargs)
    await w.start()
    """
    shutdown_event = asyncio.Event()
    
    async def report_task_coro():
        retries_left = num_retries
        while True:
            if shutdown_event.is_set():
                break

            await asyncio.sleep(report_interval)
            await send_socket.send(AckReportRequest(cluster_name=cluster_name,
                                                    worker_id=worker_id,
                                                    occupants=[]))  # TODO

            while True:
                try:
                    await asyncio.wait_for(send_socket.recv(), timeout)
                except asyncio.TimeoutError:
                    if retries_left <= 0:
                        logger.error("Could not reach manager, assuming death. Exiting!")
                        sys.exit(2)
                    else:
                        logger.warning(f"Could not reach manager, {retries_left} retries left.")
                        retries_left -= 1

                else:
                    break

    report_task = asyncio.create_task(report_task_coro())

    try:
        await asyncio.wait_for(send_socket.recv(), timeout)

    except asyncio.TimeoutError:
        logger.error(f"Could not connect to manager ({manager_addr}) after {timeout} seconds, exiting!")
        sys.exit(1)

    logger.info(f"Successfully registered worker at {manager_addr}.")

    execution = ExecutionManager(num_slots, send_socket)

    while True:
        request_msg = await recv_socket.recv()
        request_msg = BaseMessage.deserialize(request_msg)

        if isinstance(request_msg, ShutdownRequest):
            shutdown_event.set()

            await recv_socket.send(AckResponse.with_success(request_msg))

            send_socket.close()
            recv_socket.close()
            context.destroy()

            await report_task
            break

        try:
            if isinstance(request_msg, QueueRunRequest):
                await execution.queue_run(request_msg.run)
            elif isinstance(request_msg, StopRunRequest):
                await execution.stop_run(request_msg.run_id)
            else:
                logger.error(f"Unknown message {request_msg}!")
        except Exception as e:
            await recv_socket.send(AckResponse.with_exception(request_msg, e))
        else:
            await recv_socket.send(AckResponse.with_success(request_msg))
    """


@click.command
@click.option("--cluster-name", required=True, type=str)
@click.option("--worker-id", required=True, type=str)
@click.option("--manager-addr", required=True, type=str)
@click.option("--num-slots", required=True, type=int)
@click.option("--interface", required=True, type=int)
@click.option("--report-interval", type=int, default=30)
@click.option("--timeout", type=int, default=30)
@click.option("--num-retries", type=int, default=3)
def main(**kwargs):
    asyncio.run(worker(**kwargs))


if __name__ == '__main__':
    main()
