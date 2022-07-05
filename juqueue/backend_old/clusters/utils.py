from __future__ import annotations

import pickle
import typing
from dataclasses import dataclass
from typing import Union

if typing.TYPE_CHECKING:
    from juqueue.backend.utils import RunEvent


@dataclass
class ExecutionResult:
    event: RunEvent
    reason: Union[None, int, Exception] = None

    @classmethod
    def pack(cls,
             event: RunEvent,
             reason: Union[None, int, Exception] = None) -> bytes:
        return pickle.dumps(cls(event, reason))

    @classmethod
    def unpack(cls, obj) -> ExecutionResult:
        return pickle.loads(obj)


