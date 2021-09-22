import asyncio
import json
import logging
import requests
import time
import urllib.parse
import websockets
from typing import Dict, NamedTuple

_LOGGER = logging.getLogger(__name__)


def _url_transformed(m):
    result = urllib.parse.urlparse(m['url'])
    return urllib.parse.urlunparse(
        result._replace(netloc=m['headers']['bridging-base-url']))


def _ws_url_transformed(m):
    result = urllib.parse.urlparse(m['url'])
    queries = result.query.split(';')
    bridging_base_url = None
    for q in queries:
        k, v = q.split('=')
        if k == 'bridging-base-url':
            bridging_base_url = v
            break
    return urllib.parse.urlunparse(result._replace(netloc=bridging_base_url))


def deserialize_request(m: Dict) -> requests.Request:
    return requests.Request(method=m['method'],
                            url=_url_transformed(m),
                            headers=m['headers'],
                            json=m['body'])


def _eq(s1: str, s2: str):
    return s1.strip().lower() == s2.strip().lower()


class _WhiteListEntry(NamedTuple):
    method: str
    scheme: str
    netloc: str
    path: str

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return _eq(self.method, other.method) and _eq(
            self.scheme, other.scheme) and _eq(
                self.netloc, other.netloc) and _eq(self.path, other.path)

    def __repr__(self):
        return f"{self.method} {self.scheme}://{self.netloc}/{self.path}"


class _WhiteList(object):
    def __init__(self, config):
        self._entries = set()

        for entry in config:
            for netloc in entry["netloc"]:
                for method in entry["method"]:
                    for scheme in entry["scheme"]:
                        for path in entry["path"]:
                            self._entries.add(
                                _WhiteListEntry(method, scheme, netloc, path))

    def check(self, method, url):
        parsed = urllib.parse.urlparse(url)
        if _WhiteListEntry(method, parsed.scheme, parsed.netloc,
                           parsed.path) not in self._entries:
            raise Exception("forbidden")


class App(object):
    def __init__(self):
        self._ws = None
        self._wss = {}
        self._whitelist = None

    def run(self, config):
        bridge_netloc = config["bridge_netloc"]
        bridge_token = config["bridge_token"]
        self._whitelist = _WhiteList(config["whitelist"])
        while True:
            try:
                asyncio.get_event_loop().run_until_complete(
                    self._run(bridge_netloc, bridge_token))
            except Exception:
                _LOGGER.exception('')
            time.sleep(30)

    async def _run(self, bridge_netloc, bridge_token):
        async with websockets.connect(
                f"ws://{bridge_netloc}/bridge",
                extra_headers={"bridging-token": bridge_token}) as ws:
            _LOGGER.info(f'connected to gateway')
            self._ws = ws
            while True:
                msg = await ws.recv()
                _LOGGER.info(f"recv msg[{msg}]")
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
        async def _send(status_code, headers, content):
            # TODO(x): better to send raw data instead of decoded in case it's huge.
            if "Content-Encoding" in headers:
                headers.pop("Content-Encoding")
            if "Content-Length" in headers:
                headers.pop("Content-Length")
            await self._send({
                "corr_id": corr_id,
                "method": "http_result",
                "payload": {
                    "status_code": status_code,
                    "headers": headers,
                    "content": content
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
            _LOGGER.info(f"recv resp[{resp}]")

            await _send(resp.status_code, dict(resp.headers),
                        resp.content.decode())
        except Exception as e:
            _LOGGER.exception('')
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
            url = _ws_url_transformed(payload)
            self._firewall("websocket", url)
            async with websockets.connect(url) as ws:
                _LOGGER.info(f'connected url[{url}]')
                self._wss[ws_id] = ws
                await _send_result()

                try:
                    while True:
                        msg = await ws.recv()
                        _LOGGER.info(f"recv {msg}")
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
                    _LOGGER.exception('')
                    await self._send({
                        "corr_id": "0",
                        "method": "close_websocket",
                        "payload": {
                            "ws_id": ws_id
                        }
                    })

        except Exception as e:
            _LOGGER.exception('')
            await _send_result(str(e))

    def _firewall(self, method, url):
        self._whitelist.check(method, url)

    async def _send(self, msg):
        _LOGGER.info(f"send {msg}")
        await self._ws.send(json.dumps(msg))