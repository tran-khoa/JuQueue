from enum import Enum

ALL_EXPERIMENTS = "@ALL_EXPERIMENTS"
ALL_RUNS = "@ALL_RUNS"


class CancellationReason(str, Enum):
    SERVER_SHUTDOWN = "server_shutdown"
    WORKER_SHUTDOWN = "worker_shutdown"
    USER_CANCEL = "user"
