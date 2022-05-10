from typing import Any, Generic, NamedTuple, Optional, TypeVar

ResponseType = TypeVar("ResponseType")


class Response(NamedTuple, Generic[ResponseType]):
    success: bool
    reason: Optional[str] = None
    result: Optional[ResponseType] = None
