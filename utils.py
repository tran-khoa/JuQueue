from dataclasses import dataclass
from typing import Generic, Optional, TypeVar

ResponseType = TypeVar("ResponseType")


@dataclass
class Response(Generic[ResponseType]):
    success: bool
    reason: Optional[str] = None
    result: Optional[ResponseType] = None
