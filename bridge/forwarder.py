import asyncio
import base64
import gzip
import json
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
        'headers': r.headers.items(),
        'client': f"{r.client.host}:{r.client.port}",
        'body': list(await r.body())
    }


class Forwarder(object):
    def __init__(self):
        self._bridge_token = os.getenv("BRIDGE_TOKEN")
        self._compress_level = int(os.getenv("BRIDGE_COMPRESS_LEVEL", 9))
        self._bridge: Optional[WebSocket] = None
        self._reqs: Dict[str, asyncio.Future] = {}
        self._wss: Dict[str, WebSocket] = {}

    async def forward_http(self, req: Request):
        if self._bridge is None:
            return Response(status_code=503)

        if req.headers.get("bridging-base-url", None) is None:
            return Response(status_code=400)

        args = await serialize_request(req)
        result = await self._req('http', args)

        resp = Response(status_code=result["status_code"],
                        headers=dict(result["headers"]),
                        content=base64.b64decode(result["body"]))
        return resp

    async def forward_open_websocket(self, ws: WebSocket):
        if self._bridge is None:
            raise Exception('invalid')

        if "bridging-base-url" not in ws.url.query:
            raise Exception('invalid')

        ws_id = self._corr_id()
        result = await self._req(
            'open_websocket', {
                'ws_id': ws_id,
                'url': str(ws.url),
                'headers': ws.headers.items(),
                'client': f"{ws.client.host}:{ws.client.port}"
            })
        if "exception" in result:
            raise Exception(result["exception"])
        self._wss[ws_id] = ws
        return ws_id

    async def forward_websocket_msg(self, ws_id: str, msg: str):
        if self._bridge is None:
            return Response(status_code=503)

        await self._send({
            "corr_id": uuid.uuid4().hex,
            "method": "websocket_msg",
            "args": {
                "ws_id": ws_id,
                "msg": msg
            }
        })

    async def forward_close_websocket(self, ws_id: str):
        if self._bridge is None:
            return Response(status_code=503)

        if ws_id in self._wss:
            self._wss.pop(ws_id)
            await self._req('close_websocket', {'ws_id': ws_id})

    async def serve(self, ws: WebSocket):
        if self._bridge is not None:
            _LOGGER.info(f"duplicate bridge ws connection client[{ws.client}]")
            return

        if ws.headers.get('bridging-token', None) != self._bridge_token:
            _LOGGER.info(f"invalid bridge token client[{ws.client}]")
            return

        await ws.accept()
        self._bridge = ws

        try:
            while True:
                msg = await ws.receive_bytes()
                msg = json.loads(gzip.decompress(msg))
                corr_id = msg['corr_id']
                args = msg['args']
                method = msg['method']
                if method == 'close_websocket':
                    _LOGGER.info(f"recv {msg}")
                    ws_id = args['ws_id']
                    if ws_id in self._wss:
                        await self._wss.pop(ws_id).close()
                elif method == 'websocket_msg':
                    _LOGGER.debug(f"recv {msg}")
                    ws_id = args["ws_id"]
                    p_msg = args["msg"]
                    if ws_id in self._wss:
                        await self._wss[ws_id].send_text(p_msg)
                elif corr_id in self._reqs:
                    _LOGGER.info(f"recv {msg}")
                    self._reqs.pop(corr_id).set_result(args)

        except Exception:
            _LOGGER.exception('')
            self._bridge = None
            for req in self._reqs.values():
                req.set_exception(Exception("disconnected"))
            wss = self._wss
            self._wss.clear()
            for ws in wss.values():
                await ws.close()

    async def _req(self, method, args):
        corr_id = self._corr_id()
        fut = asyncio.get_running_loop().create_future()
        self._reqs[corr_id] = fut
        msg = {'corr_id': corr_id, 'method': method, 'args': args}
        try:
            await self._send(msg)
        except Exception as e:
            self._reqs.pop(corr_id).set_exception(e)

        await fut
        return fut.result()

    async def _send(self, msg):
        _LOGGER.info(f"send {msg}")
        await self._bridge.send_bytes(
            gzip.compress(bytes(json.dumps(msg), 'utf-8'),
                          compresslevel=self._compress_level))

    @staticmethod
    def _corr_id():
        return uuid.uuid4().hex
