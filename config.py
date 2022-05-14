from datetime import timedelta
from pathlib import Path


class Config:
    ROOT_DIR: Path = Path(__file__).parent
    WORK_DIR: Path = Path("juqueue")
    SOCKET_ADDRESS: str = f"ipc://{WORK_DIR.as_posix()}/server.sock"

    HEARTBEAT_INTERVAL: int = 30  # seconds
    CLUSTER_ADAPT_INTERVAL: timedelta = timedelta(minutes=1)

