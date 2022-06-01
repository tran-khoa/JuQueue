import dataclasses

from fastapi import APIRouter

from juqueue.backend.backend import Backend


router = APIRouter(tags=["Experiment"])


@router.get("/experiments")
async def get_experiments():
    result = {}

    for em in Backend.instance().experiment_managers.values():
        result[em.experiment_name] = {
            "runs": [{"def": dataclasses.asdict(run.run_def),
                      "status": run.status} for run in em.runs]
        }

    return result

