from datetime import datetime
from typing import Dict

from pydantic import BaseModel, Field

from juqueue import RunDef
from juqueue.backend.utils import RunStatus


class RunId(str):
    """
    Locally identifies a run inside an experiment
    """
    @classmethod
    def validate(cls, v):
        return v


class Run(BaseModel):
    """
    Current instance of a run definition
    """
    status: RunStatus = Field(description="Current status of the run.")
    run_def: RunDef = Field()
    created_at: datetime = Field(description="Date and time of creation.")

    class Config:
        orm_mode = True


class Experiment(BaseModel):
    """
    Logical group of runs
    """
    runs: Dict[RunId, Run] = Field(description="List of runs associated with this experiment.")
