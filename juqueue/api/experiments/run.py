from typing import List

from fastapi import APIRouter, Body
from loguru import logger
from pydantic import BaseModel

from juqueue.backend.backend import Backend

from ..utils import SuccessResponse


router = APIRouter(tags=["Experiment"])


class RunList(BaseModel):
    runs: List[str]


@router.put("/experiment/{experiment_name}/resume_runs", response_model=SuccessResponse)
async def resume_runs(experiment_name: str, run_ids: List[str] = Body()):
    """
    Resumes runs of given experiment if not running already

    - **run_ids**: List of run identifiers
    """
    try:
        await Backend.instance().experiment_managers[experiment_name].resume_runs(run_ids)
    except Exception as ex:
        logger.exception(f"An exception occured in resume_runs({experiment_name}, {run_ids})")

        return SuccessResponse.from_exception(ex)
    else:
        return SuccessResponse.with_success()


@router.put("/experiment/{experiment_name}/cancel_runs", response_model=SuccessResponse)
async def cancel_runs(experiment_name: str, run_ids: List[str] = Body(), force: bool = Body()):
    """
    Cancels runs of given experiment.

    - **run_ids**: List of run identifiers
    - **force**: Forces interruption of runs that are already running
    """
    try:
        await Backend.instance().experiment_managers[experiment_name].cancel_runs(run_ids, force)
    except Exception as ex:
        logger.exception(f"An exception occured in cancel_runs({experiment_name}, {run_ids}, {force})")

        return SuccessResponse.from_exception(ex)
    else:
        return SuccessResponse.with_success()
