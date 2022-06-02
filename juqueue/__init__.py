from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    from backend.backend import Backend


_backend = None


def set_backend(backend: Backend):
    global _backend
    _backend = backend


def get_backend() -> Backend:
    if _backend is None:
        raise RuntimeError("Backend has not been created yet!")
    return _backend  # noqa


from .definitions import RunDef, ExperimentDef, ExecutorDef
