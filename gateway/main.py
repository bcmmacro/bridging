"""
Command:
    Development: uvicorn gateway.main:app --reload --host 0.0.0.0
    Production:  uvicorn gateway.main:app

Restriction:
1. Websocket forwards json only.
"""
import logging
from fastapi import FastAPI, Request, WebSocket

from forwarder import Forwarder

logging.basicConfig(level=logging.INFO)

_logger = logging.getLogger(__name__)
_forwarder = Forwarder()

app = FastAPI()


@app.get("/{full_path:path}")
async def serve_get(request: Request, full_path):
    _logger.info(f"recv get {request.client} {request.url}")
    return await _forwarder.forward_http(request)


@app.post("/{full_path:path}")
async def serve_post(request: Request, full_path):
    _logger.info(f"recv post {request.client} {request.url}")
    return await _forwarder.forward_http(request)


@app.options("/{full_path:path}")
async def serve_options(request: Request, full_path):
    _logger.info(f"recv options {request.client} {request.url}")
    return await _forwarder.forward_http(request)


@app.websocket("/bridge")
async def serve_bridge(ws: WebSocket):
    _logger.info(f"recv bridge")
    await _forwarder.serve(ws)


@app.websocket("/{full_path:path}")
async def serve_websocket(ws: WebSocket):
    _logger.info(f"recv websocket {ws.client} {ws.url}")
    await ws.accept()

    ws_id = await _forwarder.forward_open_websocket(ws)
    _logger.info(f"opened remote websocket {ws.client} {ws_id}")
    try:
        while True:
            msg = await ws.receive_json()
            _logger.info(f"recv {ws.client} {msg}")
            await _forwarder.forward_websocket_msg(ws_id, msg)
    except Exception:
        _logger.exception(f'{ws.client}')
        await _forwarder.forward_close_websocket(ws_id)
