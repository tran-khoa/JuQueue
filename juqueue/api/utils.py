from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class SuccessResponse(BaseModel):
    success: bool
    reason: Optional[str] = None

    @classmethod
    def from_exception(cls, ex: Exception) -> SuccessResponse:
        message = str(ex.args[0]) if len(ex.args) > 0 else None
        return cls(success=False,
                   reason=message)

    @classmethod
    def with_success(cls) -> SuccessResponse:
        return cls(success=True)
