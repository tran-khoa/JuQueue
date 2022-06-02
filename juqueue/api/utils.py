from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class SuccessResponse(BaseModel):
    success: bool
    reason: Optional[str] = None

    @classmethod
    def from_exception(cls, ex: Exception) -> SuccessResponse:
        return cls(success=False,
                   reason=repr(ex))

    @classmethod
    def with_success(cls) -> SuccessResponse:
        return cls(success=True)
