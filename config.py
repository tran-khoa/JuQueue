from pathlib import Path


class Config:
    ROOT_DIR: Path = Path(__file__).parent
    WORK_DIR: Path = Path("juqueue")
    SOCKET_ADDRESS: str = f"ipc://{WORK_DIR.as_posix()}/server.sock"
