"""
uvicorn --host 0.0.0.0 --env-file .env main:app
"""
import logging
import os

from fastapi import FastAPI, Request, WebSocket
from starlette.middleware.cors import CORSMiddleware

from forwarder import Forwarder

logging.basicConfig(level=logging.INFO)
logging.getLogger('uvicorn').propagate = False

_LOGGER = logging.getLogger(__name__)
_forwarder = Forwarder()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("BRIDGE_CORS_ALLOW_ORIGINS").split(','),
    allow_credentials=True,
    allow_methods=os.getenv("BRIDGE_CORS_ALLOW_METHODS").split(','),
    allow_headers=os.getenv("BRIDGE_CORS_ALLOW_HEADERS").split(','))


@app.get("/{full_path:path}")
async def serve_get(request: Request, full_path):
    _LOGGER.info(f"recv get {request.client} {request.url}")
    return await _forwarder.forward_http(request)


@app.post("/{full_path:path}")
async def serve_post(request: Request, full_path):
    _LOGGER.info(f"recv post {request.client} {request.url}")
    return await _forwarder.forward_http(request)


@app.put("/{full_path:path}")
async def serve_put(request: Request, full_path):
    _LOGGER.info(f"recv put {request.client} {request.url}")
    return await _forwarder.forward_http(request)


@app.patch("/{full_path:path}")
async def serve_patch(request: Request, full_path):
    _LOGGER.info(f"recv patch {request.client} {request.url}")
    return await _forwarder.forward_http(request)


@app.delete("/{full_path:path}")
async def serve_delete(request: Request, full_path):
    _LOGGER.info(f"recv delete {request.client} {request.url}")
    return await _forwarder.forward_http(request)


@app.websocket("/bridge")
async def serve_bridge(ws: WebSocket):
    _LOGGER.info(f"recv bridge")
    await _forwarder.serve(ws)


@app.websocket("/{full_path:path}")
async def serve_websocket(ws: WebSocket):
    _LOGGER.info(f"recv websocket {ws.client} {ws.url}")
    await ws.accept()

    ws_id = await _forwarder.forward_open_websocket(ws)
    _LOGGER.info(f"opened remote websocket {ws.client} {ws_id}")
    try:
        while True:
            msg = await ws.receive_text()
            _LOGGER.info(f"recv {ws.client} {msg}")
            await _forwarder.forward_websocket_msg(ws_id, msg)
    except Exception:
        _LOGGER.exception(f'{ws.client}')
        await _forwarder.forward_close_websocket(ws_id)
