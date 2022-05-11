from typing import Any, Generic, NamedTuple, Optional, TypeVar
from dataclasses import dataclass


ResponseType = TypeVar("ResponseType")


@dataclass
class Response(Generic[ResponseType]):
    success: bool
    reason: Optional[str] = None
    result: Optional[ResponseType] = None
