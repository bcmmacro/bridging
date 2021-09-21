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
        result._replace(netloc=m['headers']['bridging-base-url']))


def deserialize_request(m: Dict) -> requests.Request:
    return requests.Request(method=m['method'],
                            url=url_transformed(m),
                            headers=m['headers'],
                            json=m['body'])


class App(object):
    def __init__(self):
        self._ws = None
        self._wss = {}
        self._whitelist = []

    def run(self, config=None):
        if config is not None:
            with open(config, 'r') as f:
                for l in f:
                    method, url = l.split()
                    self._whitelist.append(urllib.parse.urlparse(url))
        while True:
            try:
                asyncio.get_event_loop().run_until_complete(self._run())
            except Exception:
                _logger.exception('')
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
                    await self._handle_http(corr_id, payload)
                elif method == 'open_websocket':
                    asyncio.get_running_loop().create_task(
                        self._open_websocket(corr_id, payload))
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

    async def _handle_http(self, corr_id, payload):
        async def _send(status_code, headers, text):
            await self._send({
                "corr_id": corr_id,
                "method": "http_result",
                "payload": {
                    "status_code": status_code,
                    "headers": headers,
                    "text": text
                }
            })

        try:
            req = deserialize_request(payload)
        except Exception:
            await _send(400, {}, "")
            return

        try:
            self._firewall(req.method, req.url)
        except Exception:
            await _send(403, {}, "")
            return

        try:
            resp = requests.Session().send(req.prepare())
            _logger.info(f"recv resp[{resp}] {resp.text}")

            await _send(resp.status_code, dict(resp.headers), resp.text)
        except Exception as e:
            _logger.exception('')
            await _send(500, {}, str(e))

    async def _open_websocket(self, corr_id, payload):
        ws_id = payload["ws_id"]

        async def _send_result(exception=None):
            msg = {
                "corr_id": corr_id,
                "method": "open_websocket_result",
                "payload": {
                    "ws_id": ws_id
                }
            }
            if exception is not None:
                msg["payload"]["exception"] = exception
            await self._send(msg)

        try:
            url = url_transformed(payload)
            self._firewall(url)
            async with websockets.connect(url) as ws:
                _logger.info(f'connected url[{url}]')
                self._wss[ws_id] = ws
                await _send_result()

                try:
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
                except Exception:
                    _logger.exception('')
                    await self._send({
                        "corr_id": "0",
                        "method": "close_websocket",
                        "payload": {
                            "ws_id": ws_id
                        }
                    })

        except Exception as e:
            _logger.exception('')
            await _send_result(str(e))

    def _firewall(self, method, url):
        if len(self._whitelist) == 0:
            return

        parsed = urllib.parse.urlparse(url)
        for wl in self._whitelist:
            if method == wl.method and parsed.scheme == wl.scheme and parsed.netloc == wl.netloc and parsed.path == wl.path:
                return
        raise Exception("not allowed")

    async def _send(self, msg):
        _logger.info(f"send {msg}")
        await self._ws.send(json.dumps(msg))
