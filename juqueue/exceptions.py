from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    from juqueue.backend.utils import CancellationReason
    from juqueue.definitions import RunDef


class NoSlotsError(Exception):
    pass


class RunCancelledEvent(Exception):
    reason: CancellationReason
    run: RunDef

    def __init__(self, reason: CancellationReason, run: RunDef):
        super(RunCancelledEvent, self).__init__(f"Actor stopped {run} with reason {reason}.")
        self.reason = reason
        self.run = run


class InvalidRunDefError(Exception):
    pass


class NodeDeathError(Exception):
    pass


class NodeNotReadyError(Exception):
    pass
