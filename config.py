import os
from pathlib import Path


class Config:
    ROOT_DIR: Path = Path(__file__).parent
    WORK_DIR: Path = Path("~/.juqueue")
