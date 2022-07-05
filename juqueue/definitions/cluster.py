from pydantic import BaseModel
from enum import Enum


class ClusterType(str, Enum):
    LOCAL = "local"
    SLURM = "slurm"


class ClusterDef(BaseModel):
    name: str
    max_jobs: int
    num_slots: int
    scheduler_interface: str
    type: ClusterType

    class Config:
        use_enum_values = True
