import asyncio
import logging
import os
import uuid
from typing import Optional, Dict

from fastapi import Request, Response, WebSocket

_LOGGER = logging.getLogger(__name__)


async def serialize_request(r: Request) -> Dict:
    return {
        'method': r.method,
        'url': str(r.url),
        'headers': dict(r.headers.items()),
        'client': r.client._asdict(),
        'body': (await r.body()).decode()
    }


class Forwarder(object):
    def __init__(self):
        self._bridge_token = os.getenv("GATEWAY_BRIDGE_TOKEN")
        self._ws: Optional[WebSocket] = None
        self._reqs: Dict[str, asyncio.Future] = {}
        self._wss: Dict[str, WebSocket] = {}

    async def forward_http(self, req: Request):
        payload = await serialize_request(req)
        result = await self._req('http', payload)
        resp = Response(status_code=result["status_code"],
                        headers=result["headers"],
                        content=str.encode(result["content"]))
        return resp

    async def forward_open_websocket(self, ws: WebSocket):
        ws_id = self._corr_id()
        result = await self._req(
            'open_websocket', {
                'ws_id': ws_id,
                'url': str(ws.url),
                'headers': dict(ws.headers.items()),
                'client': ws.client._asdict()
            })
        if "exception" in result:
            raise Exception(result["exception"])
        self._wss[ws_id] = ws
        return ws_id

    async def forward_websocket_msg(self, ws_id: str, json: Dict):
        await self._ws.send_json({
            "corr_id": "0",
            "method": "websocket_msg",
            "payload": {
                "ws_id": ws_id,
                "msg": json
            }
        })

    async def forward_close_websocket(self, ws_id: str):
        if ws_id in self._wss:
            await self._req('close_websocket', {'ws_id': ws_id})
            self._wss.pop(ws_id)

    async def serve(self, ws: WebSocket):
        if self._ws is not None:
            _LOGGER.info(f"duplicate bridge ws connection client[{ws.client}]")
            return

        if ws.headers.get('bridging-token', None) != self._bridge_token:
            _LOGGER.info(f"invalid bridge token client[{ws.client}]")
            return

        await ws.accept()
        self._ws = ws

        try:
            while True:
                msg = await ws.receive_json()
                _LOGGER.info(f"recv {msg}")
                corr_id = msg['corr_id']
                payload = msg['payload']
                if corr_id == '0':
                    method = msg['method']
                    if method == 'close_websocket':
                        ws_id = payload['ws_id']
                        if ws_id in self._wss:
                            await self._wss.pop(ws_id).close()
                    elif method == 'websocket_msg':
                        ws_id = payload["ws_id"]
                        msg = payload["msg"]
                        if ws_id in self._wss:
                            await self._wss[ws_id].send_json(msg)
                elif corr_id in self._reqs:
                    self._reqs.pop(corr_id).set_result(payload)

        except Exception:
            _LOGGER.exception('')
            self._ws = None
            for req in self._reqs.values():
                req.set_exception(Exception("disconnected"))
            wss = self._wss
            self._wss.clear()
            for ws in wss.values():
                await ws.close()

    async def _req(self, method, payload):
        corr_id = self._corr_id()
        fut = asyncio.get_running_loop().create_future()
        self._reqs[corr_id] = fut
        msg = {'corr_id': corr_id, 'method': method, 'payload': payload}
        _LOGGER.info(f"send {msg}")
        await self._ws.send_json(msg)
        await fut
        return fut.result()

    @staticmethod
    def _corr_id():
        return uuid.uuid4().hex
