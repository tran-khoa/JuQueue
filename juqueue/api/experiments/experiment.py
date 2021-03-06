from typing import Dict

from fastapi import APIRouter

from juqueue.backend.backend import Backend
from juqueue.models import Experiment, Run

router = APIRouter(tags=["Experiment"])


@router.get("/experiments",
            response_model=Dict[str, Experiment],
            name="Get experiments")
async def get_experiments():
    """
    Returns all experiments.
    """
    result = {}

    for em in Backend.instance().experiment_managers.values():
        result[em.experiment_name] = Experiment(
            runs={run_id: Run.from_orm(run) for run_id, run in em.runs.items()}
        )

    return result

