from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class RunStatus(str, Enum):
    RUNNING = 'running'
    READY = 'ready'
    FAILED = 'failed'
    INACTIVE = 'inactive'
    WAITING_DEP = 'waiting_dep'
    FINISHED = 'finished'


@dataclass
class RunState:
    run_id: str
    status: RunStatus = RunStatus.INACTIVE
    created_at: datetime = field(default_factory=lambda: datetime.now())
