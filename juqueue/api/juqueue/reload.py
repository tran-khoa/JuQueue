from fastapi import APIRouter

from juqueue.backend.backend import Backend
from ..utils import SuccessResponse

router = APIRouter(tags=["JuQueue"])


@router.put("/juqueue/reload_experiments",
            response_model=SuccessResponse)
async def reload_experiments():
    try:
        await Backend.instance().load_experiments()
    except Exception as ex:
        return SuccessResponse.from_exception(ex)
    return SuccessResponse.with_success()


@router.put("/juqueue/reload_clusters",
            response_model=SuccessResponse)
async def reload_clusters():
    try:
        await Backend.instance().load_clusters()
    except Exception as ex:
        return SuccessResponse.from_exception(ex)
    return SuccessResponse.with_success()
