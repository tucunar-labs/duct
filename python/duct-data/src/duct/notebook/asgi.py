"""marimo as an ASGI app behind Duct authentication (ADR 0006).

The guard is a *pure ASGI* middleware, not Starlette's ``BaseHTTPMiddleware``: that one only
wraps HTTP, and marimo runs notebook code over a WebSocket, so the WebSocket is the part
that must never be reachable without credentials. Every ``http`` and ``websocket`` scope is
authenticated before it reaches the wrapped app; anything without valid credentials gets
401 (HTTP, with ``WWW-Authenticate``) or is closed before ``accept`` (WebSocket, which the
server turns into HTTP 403).

Credentials, in order: ``Authorization: Bearer <token>``, ``Authorization: Basic`` (user and
password, so browsers can prompt), the ``duct_nb`` cookie, and ``?token=<token>`` (which also
sets the cookie, so the browser keeps it for the WebSocket). They are checked by the Go core
(``duct serve``'s notebook protocol) over a loopback endpoint: this process never sees the
lake or the credential store. Run it with
``python -m duct.notebook.asgi --dir notebooks --port 8124 --auth-url http://127.0.0.1:N/auth``.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import hashlib
import html
import http.cookies
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Awaitable, Callable, MutableMapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]

COOKIE = "duct_nb"
AUTH_HEADER = "X-Duct-Auth"
ENV_AUTH_SECRET = "DUCT_NOTEBOOK_AUTH_SECRET"
_SAFE = {"GET", "HEAD", "OPTIONS"}


@dataclass(frozen=True)
class Identity:
    """Who the Duct core says the caller is."""

    user: str
    role: str


Validator = Callable[[str], Identity | None]
"""Takes an ``Authorization`` header value (``Bearer ...`` / ``Basic ...``); ``None`` = invalid."""


def http_validator(auth_url: str, secret: str, timeout: float = 5.0) -> Validator:
    """Validate credentials against the Go core's loopback ``/auth`` endpoint."""

    def validate(authorization: str) -> Identity | None:
        req = urllib.request.Request(
            auth_url,
            data=b"",
            method="POST",
            headers={"Authorization": authorization, AUTH_HEADER: secret},
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = json.loads(resp.read())
        except urllib.error.HTTPError:
            return None  # 401: bad credentials (anything else also fails closed)
        except (urllib.error.URLError, OSError, ValueError):
            return None
        user, role = body.get("user"), body.get("role")
        if not isinstance(user, str) or not isinstance(role, str):
            return None
        return Identity(user, role)

    return validate


def _headers(scope: Scope) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in scope.get("headers", []):
        out.setdefault(k.decode("latin-1").lower(), v.decode("latin-1"))
    return out


def _same_origin(headers: dict[str, str]) -> bool:
    origin = headers.get("origin")
    if origin is None:
        return True
    host = headers.get("host", "")
    return urllib.parse.urlsplit(origin).netloc.lower() == host.lower() and host != ""


class DuctAuthMiddleware:
    """Pure ASGI authentication for ``http`` and ``websocket`` scopes."""

    def __init__(
        self,
        app: ASGIApp,
        validate: Validator,
        *,
        allow_roles: frozenset[str] | None = None,
        cache_ttl: float = 15.0,
        max_cache: int = 1024,
    ) -> None:
        self.app = app
        self._validate = validate
        self._allow = allow_roles
        self._ttl = cache_ttl
        self._max = max_cache
        self._cache: dict[str, tuple[float, Identity | None]] = {}

    async def _identity(self, authorization: str) -> Identity | None:
        key = hashlib.sha256(authorization.encode()).hexdigest()
        hit = self._cache.get(key)
        now = time.monotonic()
        if hit is not None and hit[0] > now:
            return hit[1]
        ident = await asyncio.to_thread(self._validate, authorization)
        if len(self._cache) >= self._max:
            self._cache.clear()
        # Failures are cached too, briefly, so a flood of bad tokens is not forwarded 1:1.
        self._cache[key] = (now + (self._ttl if ident else min(self._ttl, 2.0)), ident)
        return ident

    @staticmethod
    def _credential(scope: Scope, headers: dict[str, str]) -> tuple[str | None, str | None]:
        """Return (Authorization value, token to set as cookie)."""
        auth = headers.get("authorization", "")
        if auth.lower().startswith(("bearer ", "basic ")):
            return auth, None
        jar = http.cookies.SimpleCookie()
        with contextlib.suppress(http.cookies.CookieError):
            jar.load(headers.get("cookie", ""))
        if COOKIE in jar and jar[COOKIE].value:
            return "Bearer " + jar[COOKIE].value, None
        qs = urllib.parse.parse_qs(scope.get("query_string", b"").decode("latin-1"))
        tok = qs.get("token", [""])[0]
        if tok:
            return "Bearer " + tok, tok
        return None, None

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        kind = scope["type"]
        if kind not in ("http", "websocket"):  # lifespan
            await self.app(scope, receive, send)
            return
        headers = _headers(scope)
        if kind == "websocket":
            await receive()  # websocket.connect
        if (kind == "websocket" or scope["method"] not in _SAFE) and not _same_origin(headers):
            await self._deny(kind, send, 403, "cross-origin request refused")
            return
        auth, set_cookie = self._credential(scope, headers)
        ident = await self._identity(auth) if auth else None
        if ident is None:
            await self._deny(kind, send, 401, "authentication required")
            return
        if self._allow is not None and ident.role not in self._allow:
            await self._deny(kind, send, 403, "role not allowed")
            return
        scope["duct.identity"] = ident
        if kind == "websocket":
            # The wrapped app expects to read its own websocket.connect first.
            first = True

            async def replay() -> Message:
                nonlocal first
                if first:
                    first = False
                    return {"type": "websocket.connect"}
                return await receive()

            await self.app(scope, replay, send)
            return
        if set_cookie is None:
            await self.app(scope, receive, send)
            return

        async def with_cookie(message: Message) -> None:
            if message["type"] == "http.response.start":
                value = f"{COOKIE}={set_cookie}; Path=/; HttpOnly; SameSite=Strict"
                message["headers"] = [*message.get("headers", []), (b"set-cookie", value.encode())]
            await send(message)

        await self.app(scope, receive, with_cookie)

    @staticmethod
    async def _deny(kind: str, send: Send, status: int, why: str) -> None:
        if kind == "websocket":
            # Closing before accept makes the server answer 403 to the upgrade.
            await send({"type": "websocket.close", "code": 1008, "reason": why})
            return
        body = (why + "\n").encode()
        headers = [
            (b"content-type", b"text/plain; charset=utf-8"),
            (b"content-length", str(len(body)).encode()),
        ]
        if status == 401:
            headers.append((b"www-authenticate", b'Basic realm="duct", charset="UTF-8"'))
        await send({"type": "http.response.start", "status": status, "headers": headers})
        await send({"type": "http.response.body", "body": body})


def basic_header(user: str, password: str) -> str:
    """``Authorization`` value for HTTP Basic (handy in tests and scripts)."""
    return "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()


def create_app(
    directory: str,
    validate: Validator,
    *,
    allow_roles: frozenset[str] | None = None,
    mode: str = "run",
) -> ASGIApp:
    """marimo over ``directory`` behind :class:`DuctAuthMiddleware`.

    ``mode="run"`` serves each ``*.py`` notebook as a read-only app (``/<name>``), the safe
    default: visitors run the project's code, not their own. Needs ``duct-data[marimo]``.
    """
    if mode != "run":
        raise ValueError("only mode='run' is supported (edit locally with `duct notebook`)")
    import marimo  # imported here: heavy, and optional

    marimo_app: ASGIApp = (
        marimo.create_asgi_app().with_dynamic_directory(path=MOUNT, directory=directory).build()
    )
    return DuctAuthMiddleware(_with_index(marimo_app, directory), validate, allow_roles=allow_roles)


MOUNT = "/notebooks"


def _with_index(app: ASGIApp, directory: str) -> ASGIApp:
    """``/`` lists the notebooks (marimo itself mounts them under ``/notebooks/<name>``)."""

    async def wrapper(scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["path"] not in ("", "/"):
            await app(scope, receive, send)
            return
        names = sorted(p.stem for p in Path(directory).glob("*.py") if not p.stem.startswith("_"))
        links = "".join(
            f'<li><a href="{MOUNT}/{html.escape(n)}">{html.escape(n)}</a></li>' for n in names
        )
        page = f"<!doctype html><title>Duct notebooks</title><h1>Notebooks</h1><ul>{links}</ul>"
        body = page.encode()
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"text/html; charset=utf-8"),
                    (b"content-length", str(len(body)).encode()),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})

    return wrapper


def is_loopback(host: str) -> bool:
    return host in ("127.0.0.1", "::1", "localhost")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m duct.notebook.asgi")
    ap.add_argument("--dir", required=True, help="folder with marimo notebooks")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--auth-url", required=True, help="Duct core endpoint that validates tokens")
    ap.add_argument("--allow-role", action="append", default=[], help="only these roles may enter")
    ap.add_argument("--mode", default="run")
    args = ap.parse_args(argv)
    if not is_loopback(args.host):
        raise SystemExit("refusing to listen on a non-loopback address (put a TLS proxy in front)")
    secret = os.environ.pop(ENV_AUTH_SECRET, "")
    if not secret:
        raise SystemExit(f"{ENV_AUTH_SECRET} is required")
    import uvicorn

    app = create_app(
        args.dir,
        http_validator(args.auth_url, secret),
        allow_roles=frozenset(args.allow_role) or None,
        mode=args.mode,
    )
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning", ws="auto")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
