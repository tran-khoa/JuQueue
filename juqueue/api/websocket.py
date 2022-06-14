import asyncio
import uuid

from fastapi import APIRouter, WebSocket

from juqueue.backend.backend import Backend

router = APIRouter()


@router.websocket("/ws", name="ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    name = str(uuid.uuid4())
    observer = asyncio.Event()
    Backend.instance().register_observer(name, observer)

    try:
        while True:
            await observer.wait()
            await websocket.send_text("update")
            observer.clear()
    finally:
        Backend.instance().unregister_observer(name)
