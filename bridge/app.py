import asyncio
import json

import logging
import requests
import time
import urllib.parse
import websockets
from typing import Dict

_logger = logging.getLogger(__name__)


def url_transformed(m):
    result = urllib.parse.urlparse(m['url'])
    return urllib.parse.urlunparse(
        result._replace(netloc=m['headers']['briding-base-url']))


def deserialize_request(m: Dict) -> requests.Request:
    return requests.Request(method=m['method'],
                            url=url_transformed(m),
                            headers=m['headers'],
                            json=m['body'])


class App(object):
    def __init__(self):
        self._ws = None
        self._wss = {}

    def run(self):
        while True:
            asyncio.get_event_loop().run_until_complete(self._run())
            time.sleep(30)

    async def _run(self):
        async with websockets.connect('ws://0.0.0.0:8000/bridge') as ws:
            _logger.info(f'connected to gateway')
            self._ws = ws
            while True:
                msg = await ws.recv()
                _logger.info(f"recv msg[{msg}]")
                msg = json.loads(msg)
                corr_id = msg["corr_id"]
                method = msg["method"]
                payload = msg["payload"]

                if method == 'http':
                    req = deserialize_request(payload)
                    # TODO(zzl) firewall
                    resp = requests.Session().send(req.prepare())
                    _logger.info(f"recv resp[{resp}] {resp.text}")

                    await self._send({
                        "corr_id": corr_id,
                        "method": "http_result",
                        "payload": {
                            "status_code": resp.status_code,
                            "headers": dict(resp.headers),
                            "text": resp.text
                        }
                    })
                elif method == 'open_websocket':
                    asyncio.get_running_loop().create_task(
                        self._new_websocket(corr_id, payload))
                elif method == 'close_websocket':
                    try:
                        ws_id = payload["ws_id"]
                        if ws_id in self._wss:
                            await self._wss.pop(ws_id).close()
                        await self._send({
                            "corr_id": corr_id,
                            "method": "close_websocket_result",
                            "payload": {
                                "ws_id": ws_id
                            }
                        })
                    except Exception as e:
                        await self._send({
                            "corr_id": corr_id,
                            "method": "close_websocket_result",
                            "payload": {
                                "ws_id": ws_id,
                                "exception": str(e)
                            }
                        })
                elif method == "websocket_msg":
                    ws_id = payload["ws_id"]
                    msg = payload["msg"]
                    if ws_id in self._wss:
                        await self._wss[ws_id].send(json.dumps(msg))

    async def _new_websocket(self, corr_id, payload):
        connected = False
        ws_id = payload["ws_id"]
        try:
            url = url_transformed(payload)
            # TODO(zzl) firewall
            async with websockets.connect(url) as ws:
                _logger.info(f'connected url[{url}]')
                self._wss[ws_id] = ws
                await self._send({
                    "corr_id": corr_id,
                    "payload": {
                        "ws_id": ws_id
                    }
                })

                while True:
                    msg = await ws.recv()
                    _logger.info(f"recv {msg}")
                    msg = json.loads(msg)
                    await self._send({
                        "corr_id": "0",
                        "method": "websocket_msg",
                        "payload": {
                            "ws_id": ws_id,
                            "msg": msg
                        }
                    })

        except Exception as e:
            if not connected:
                await self._send({
                    "corr_id": corr_id,
                    "method": "open_websocket_result",
                    "payload": {
                        "ws_id": ws_id,
                        "exception": str(e)
                    }
                })
            else:
                await self._send({
                    "corr_id": "0",
                    "method": "close_websocket",
                    "payload": {
                        "ws_id": ws_id
                    }
                })

    async def _send(self, msg):
        _logger.info(f"send {msg}")
        await self._ws.send(json.dumps(msg))
