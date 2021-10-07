import asyncio
import gzip
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
    queries = result.query.split('&')
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
                            data=m['body'])


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
        self._bridge = None
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
                f"{bridge_netloc}/bridge",
                extra_headers={"bridging-token": bridge_token}) as ws:
            _LOGGER.info(f'connected to bridge')
            self._bridge = ws
            while True:
                msg = await ws.recv()
                msg = json.loads(gzip.decompress(msg))
                _LOGGER.info(f"recv bridge msg[{msg}]")

                corr_id = msg["corr_id"]
                method = msg["method"]
                args = msg["args"]

                if method == 'http':
                    await self._handle_http(corr_id, args)
                elif method == 'open_websocket':
                    asyncio.get_running_loop().create_task(
                        self._open_websocket(corr_id, args))
                elif method == 'close_websocket':
                    try:
                        ws_id = args["ws_id"]
                        if ws_id in self._wss:
                            await self._wss.pop(ws_id).close()
                        await self._send({
                            "corr_id": corr_id,
                            "method": "close_websocket_result",
                            "args": {
                                "ws_id": ws_id
                            }
                        })
                    except Exception as e:
                        await self._send({
                            "corr_id": corr_id,
                            "method": "close_websocket_result",
                            "args": {
                                "ws_id": ws_id,
                                "exception": str(e)
                            }
                        })
                elif method == "websocket_msg":
                    ws_id = args["ws_id"]
                    msg = args["msg"]
                    if ws_id in self._wss:
                        await self._wss[ws_id].send(msg)

    async def _handle_http(self, corr_id, args):
        async def _send(status_code, headers, content):
            # send decoded data is ok, since bridge msg is gzipped.
            if "Content-Encoding" in headers:
                headers.pop("Content-Encoding")
            if "Content-Length" in headers:
                headers.pop("Content-Length")
            await self._send({
                "corr_id": corr_id,
                "method": "http_result",
                "args": {
                    "status_code": status_code,
                    "headers": headers,
                    "content": content
                }
            })

        try:
            req = deserialize_request(args)
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
            _LOGGER.info(f"recv http resp[{resp}]")

            await _send(resp.status_code, dict(resp.headers),
                        resp.content.decode())
        except Exception as e:
            _LOGGER.exception('')
            await _send(500, {}, str(e))

    async def _open_websocket(self, corr_id, args):
        ws_id = args["ws_id"]

        async def _send_result(exception=None):
            msg = {
                "corr_id": corr_id,
                "method": "open_websocket_result",
                "args": {
                    "ws_id": ws_id
                }
            }
            if exception is not None:
                msg["args"]["exception"] = exception
            await self._send(msg)

        try:
            url = _ws_url_transformed(args)
            self._firewall("websocket", url)
            async with websockets.connect(url) as ws:
                _LOGGER.info(f'connected url[{url}]')
                self._wss[ws_id] = ws
                await _send_result()

                try:
                    while True:
                        msg = await ws.recv()
                        _LOGGER.debug(f"recv ws_id[{ws_id}] {msg}")
                        await self._send(
                            {
                                "corr_id": "0",
                                "method": "websocket_msg",
                                "args": {
                                    "ws_id": ws_id,
                                    "msg": msg
                                }
                            }, logging.DEBUG)
                except Exception:
                    _LOGGER.exception('')
                    await self._send({
                        "corr_id": "0",
                        "method": "close_websocket",
                        "args": {
                            "ws_id": ws_id
                        }
                    })

        except Exception as e:
            _LOGGER.exception('')
            await _send_result(str(e))

    def _firewall(self, method, url):
        self._whitelist.check(method, url)

    async def _send(self, msg, log_level=logging.INFO):
        _LOGGER.log(level=log_level, msg=f"send bridge {msg}")
        await self._bridge.send(gzip.compress(bytes(json.dumps(msg), 'utf-8')))
