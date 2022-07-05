from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from enum import Enum
import pickle
from typing import Any, List, Literal, Optional, TypedDict, Dict, Type

from juqueue.backend.entities.run_instance import RunInstance


@dataclass
class BaseMessage:
    def serialize(self) -> bytes:
        return pickle.dumps(self)

    @classmethod
    def deserialize(cls, message: bytes) -> BaseMessage:
        return pickle.loads(message)


@dataclass
class InternalMessage(BaseMessage):
    pass


@dataclass
class RequestMessage(BaseMessage):
    cluster_name: str
    worker_id: str


@dataclass
class ResponseMessage(BaseMessage):
    request: RequestMessage

    @property
    def cluster_name(self):
        return self.request.cluster_name

    @property
    def worker_id(self):
        return self.request.worker_id


@dataclass
class InitResponse(InternalMessage):
    address: str


@dataclass
class WorkerShutdownMessage(RequestMessage):
    pass


@dataclass
class AckReportRequest(RequestMessage):
    occupants: List[str]


@dataclass
class AckStatusRequest(RequestMessage):
    run_id: str
    status: Literal["..."]  # TODO


@dataclass
class RegisterWorkerRequest(RequestMessage):
    address: str


@dataclass
class AckResponse(ResponseMessage):
    success: bool = True
    exception: Optional[Exception] = None

    @classmethod
    def with_success(cls, request: RequestMessage) -> AckResponse:
        return cls(request)

    @classmethod
    def with_exception(cls, request: RequestMessage, exception: Exception) -> AckResponse:
        return cls(request, success=False, exception=exception)


@dataclass
class ShutdownRequest(RequestMessage):
    pass


@dataclass
class QueueRunRequest(RequestMessage):
    run: RunInstance


@dataclass
class StopRunRequest(RequestMessage):
    run_id: str


