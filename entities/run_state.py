from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Dict, Any, Tuple, Optional

from config import Config

RunStatus = Literal['running', 'pending', 'waiting', 'moving', 'failed', 'cancelled', 'finished']


@dataclass
class RunState:
    status: RunStatus = 'cancelled'
    last_run: Optional[datetime] = None
    last_error: Optional[str] = None
    last_heartbeat: Optional[datetime] = None

    def transition(self, new_status: RunStatus) -> Tuple[RunStatus, RunStatus]:
        old_status = self.status
        self.status = new_status
        return old_status, new_status

    def check_heartbeat(self) -> Tuple[RunStatus, RunStatus]:
        now = datetime.now()
        if (now - self.last_heartbeat).total_seconds() < Config.HEARTBEAT_INTERVAL * 2:
            return self.transition("running")
        else:
            return self.transition("pending")

    def reset(self):
        self.transition("pending")
        self.last_run = None
        self.last_error = None
        self.last_heartbeat = None

    def is_active(self) -> bool:
        return self.status in ('running', 'moving', 'waiting', 'pending')

    @property
    def persistable_status(self) -> Literal['pending', 'failed', 'cancelled', 'finished']:
        if self.is_active():
            return "pending"
        return self.status

    def load_state_dict(self, state_dict: Dict[str, Any]):
        self.status = state_dict['status']
        self.last_run = datetime.fromisoformat(state_dict['last_run'])
        self.last_error = state_dict['last_error']

    def state_dict(self) -> Dict[str, Any]:
        return {
            'status': self.persistable_status,
            'last_run': self.last_run.isoformat(),
            'last_error': self.last_error
        }
