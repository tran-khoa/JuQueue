from __future__ import annotations

import typing

__version__ = '0.0.12'

if typing.TYPE_CHECKING:
    from backend.backend import Backend


class BackendInstance:
    backend: Backend = None

    @classmethod
    def ready(cls) -> bool:
        return cls.backend is not None

    @classmethod
    def get(cls) -> Backend:
        if cls.backend is None:
            raise RuntimeError("Backend has not been created yet!")
        return cls.backend

    @classmethod
    def set(cls, backend: Backend):
        if cls.backend is None:
            cls.backend = backend


def get_backend() -> Backend:
    return BackendInstance.get()


from .definitions import RunDef, ExperimentDef, ExecutorDef
