import asyncio

from loguru import logger
from tornado.ioloop import IOLoop
from fastapi import APIRouter

from juqueue.backend.backend import Backend


router = APIRouter(tags=["JuQueue"])


@router.put("/juqueue/stop")
async def stop():
    await Backend.instance().stop()
    logger.remove()

    IOLoop.current().call_later(1, lambda: IOLoop.current().stop())
    asyncio.get_event_loop().set_exception_handler(None)

    return "Server stopped"
