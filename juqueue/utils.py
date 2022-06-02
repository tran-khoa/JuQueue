from enum import Enum
from pathlib import Path


class CancellationReason(str, Enum):
    SERVER_SHUTDOWN = "server_shutdown"
    WORKER_SHUTDOWN = "worker_shutdown"
    USER_CANCEL = "user"


WORK_DIR: Path = Path(__file__).parent.parent / "work"
