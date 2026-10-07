# pyright: basic
import asyncio
import socket
import threading
import time
from collections.abc import Iterator
from typing import Any

import pytest

from duct.notebook.asgi import DuctAuthMiddleware, Identity, basic_header

TOKENS = {"Bearer good": Identity("ana", "analistas"), "Bearer adm": Identity("root", "duct_admin")}
calls: list[str] = []


def validate(authorization: str) -> Identity | None:
    calls.append(authorization)
    if authorization == basic_header("ana", "pw"):
        return Identity("ana", "analistas")
    return TOKENS.get(authorization)


async def inner(scope: Any, receive: Any, send: Any) -> None:
    if scope["type"] == "websocket":
        await receive()
        await send({"type": "websocket.accept"})
        while True:
            msg = await receive()
            if msg["type"] == "websocket.disconnect":
                return
            await send({"type": "websocket.send", "text": "echo:" + msg["text"]})
    elif scope["type"] == "http":
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"hello"})


def run_http(app: Any, headers: dict[str, str] | None = None, qs: str = "", method: str = "GET"):
    sent: list[dict[str, Any]] = []

    async def go() -> None:
        async def receive() -> dict[str, Any]:
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(m: Any) -> None:
            sent.append(m)

        scope = {
            "type": "http",
            "method": method,
            "path": "/",
            "query_string": qs.encode(),
            "headers": [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()],
        }
        await app(scope, receive, send)

    asyncio.run(go())
    return sent


def test_http_without_credentials_is_401() -> None:
    app = DuctAuthMiddleware(inner, validate)
    sent = run_http(app)
    assert sent[0]["status"] == 401
    assert (b"www-authenticate", b'Basic realm="duct", charset="UTF-8"') in sent[0]["headers"]


def test_http_bearer_basic_and_bad() -> None:
    app = DuctAuthMiddleware(inner, validate)
    assert run_http(app, {"Authorization": "Bearer good"})[0]["status"] == 200
    assert run_http(app, {"Authorization": basic_header("ana", "pw")})[0]["status"] == 200
    assert run_http(app, {"Authorization": "Bearer evil"})[0]["status"] == 401
    assert run_http(app, {"Authorization": "Negotiate x"})[0]["status"] == 401


def test_query_token_sets_cookie_and_cookie_authenticates() -> None:
    app = DuctAuthMiddleware(inner, validate)
    sent = run_http(app, qs="token=good")
    assert sent[0]["status"] == 200
    cookie = dict(sent[0]["headers"])[b"set-cookie"].decode()
    assert cookie.startswith("duct_nb=good;") and "HttpOnly" in cookie and "Strict" in cookie
    assert run_http(app, {"Cookie": "duct_nb=good"})[0]["status"] == 200
    assert run_http(app, {"Cookie": "duct_nb=nope"})[0]["status"] == 401


def test_role_allowlist_and_cross_origin() -> None:
    app = DuctAuthMiddleware(inner, validate, allow_roles=frozenset({"duct_admin"}))
    assert run_http(app, {"Authorization": "Bearer good"})[0]["status"] == 403
    assert run_http(app, {"Authorization": "Bearer adm"})[0]["status"] == 200
    app = DuctAuthMiddleware(inner, validate)
    hdr = {"Authorization": "Bearer good", "Host": "x:1", "Origin": "http://evil.example"}
    assert run_http(app, hdr, method="POST")[0]["status"] == 403
    assert run_http(app, hdr, method="GET")[0]["status"] == 200  # safe methods are not CSRF


def test_validation_is_cached() -> None:
    calls.clear()
    app = DuctAuthMiddleware(inner, validate)
    for _ in range(3):
        run_http(app, {"Authorization": "Bearer good"})
    # a fresh asyncio loop per run_http, same middleware: one validation
    assert calls.count("Bearer good") == 1


def test_websocket_without_token_is_closed_before_accept() -> None:
    app = DuctAuthMiddleware(inner, validate)
    sent: list[dict[str, Any]] = []

    async def go() -> None:
        async def receive() -> dict[str, Any]:
            return {"type": "websocket.connect"}

        async def send(m: Any) -> None:
            sent.append(m)

        await app(
            {"type": "websocket", "path": "/ws", "query_string": b"", "headers": []}, receive, send
        )

    asyncio.run(go())
    assert [m["type"] for m in sent] == ["websocket.close"]


@pytest.fixture
def live() -> Iterator[int]:
    uvicorn = pytest.importorskip("uvicorn")
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    cfg = uvicorn.Config(
        DuctAuthMiddleware(inner, validate),
        host="127.0.0.1",
        port=port,
        log_level="error",
        ws="auto",
    )
    srv = uvicorn.Server(cfg)
    t = threading.Thread(target=srv.run, daemon=True)
    t.start()
    for _ in range(100):
        if srv.started:
            break
        time.sleep(0.05)
    yield port
    srv.should_exit = True
    t.join(5)


def test_live_websocket_403_without_token(live: int) -> None:
    from websockets.exceptions import InvalidStatus
    from websockets.sync.client import connect

    with pytest.raises(InvalidStatus) as e:
        connect(f"ws://127.0.0.1:{live}/ws")
    assert e.value.response.status_code == 403
    with pytest.raises(InvalidStatus) as e:
        connect(f"ws://127.0.0.1:{live}/ws", additional_headers={"Authorization": "Bearer evil"})
    assert e.value.response.status_code == 403
    with pytest.raises(InvalidStatus) as e:  # cross-origin with a valid token
        connect(
            f"ws://127.0.0.1:{live}/ws",
            additional_headers={"Authorization": "Bearer good", "Origin": "http://evil.example"},
        )
    assert e.value.response.status_code == 403


def test_live_websocket_with_token_and_cookie(live: int) -> None:
    from websockets.sync.client import connect

    with connect(
        f"ws://127.0.0.1:{live}/ws", additional_headers={"Authorization": "Bearer good"}
    ) as ws:
        ws.send("hi")
        assert ws.recv() == "echo:hi"
    with connect(f"ws://127.0.0.1:{live}/ws?token=good") as ws:
        ws.send("yo")
        assert ws.recv() == "echo:yo"
    with connect(f"ws://127.0.0.1:{live}/ws", additional_headers={"Cookie": "duct_nb=good"}) as ws:
        ws.send("c")
        assert ws.recv() == "echo:c"
