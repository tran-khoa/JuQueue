from enum import Enum
from pathlib import Path


class CancellationReason(str, Enum):
    SERVER_SHUTDOWN = "server_shutdown"
    WORKER_SHUTDOWN = "worker_shutdown"
    USER_CANCEL = "user"


ROOT_DIR: Path = Path(__file__).parent.parent
WORK_DIR: Path = ROOT_DIR / "work"
EXPERIMENTS_DIR: Path = ROOT_DIR / "experiments"
CLUSTERS_YAML: Path = ROOT_DIR / "clusters.yaml"
