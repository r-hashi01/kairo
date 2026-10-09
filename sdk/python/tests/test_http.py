"""Actions called over HTTP(S) (ADR 0052): the runtime embedded here calls
an action at its URL; the outcome comes in the answer (200), or later on
the callback (202). Needs go and wasmtime; openssl for the HTTPS test.

The ASGI apps are served by a few lines of asyncio below (no dependency)."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import ssl
import subprocess
import tempfile
import time
import unittest
from typing import Any

from kairo_sdk.backend import EmbeddedBackend
from kairo_sdk.http import check_url, post, sign
from kairo_sdk.store import SQLiteStore
from kairo_sdk.workflow import Kairo, RetryableError, Suspended, UnknownOutcomeError

from test_workflow import HAS_GO, HAS_WASMTIME, build_wasm

HAS_OPENSSL = shutil.which("openssl") is not None
SECRET = "test-secret"
SERVERS: list[asyncio.AbstractServer] = []


def run(main: Any) -> None:
    """Runs a test's main, then closes the servers it started."""

    async def go() -> None:
        try:
            await main()
        finally:
            while SERVERS:
                s = SERVERS.pop()
                s.close()
                await s.wait_closed()

    asyncio.run(go())


async def serve(app: Any, tls: ssl.SSLContext | None = None) -> tuple[str, asyncio.AbstractServer]:
    """Serves an ASGI app on this machine (HTTP/1.1, one request per connection)."""

    async def conn(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        try:
            line = (await r.readline()).decode().split()
            headers: list[tuple[bytes, bytes]] = []
            n = 0
            while (h := await r.readline()) not in (b"\r\n", b""):
                k, v = h.decode().split(":", 1)
                headers.append((k.strip().lower().encode(), v.strip().encode()))
                if k.strip().lower() == "content-length":
                    n = int(v)
            body = await r.readexactly(n)
            sent = False
            out: dict[str, Any] = {}

            async def receive() -> dict[str, Any]:
                nonlocal sent
                if sent:
                    await asyncio.Event().wait()
                sent = True
                return {"type": "http.request", "body": body, "more_body": False}

            async def send(m: dict[str, Any]) -> None:
                out.update(m) if m["type"] == "http.response.start" else out.setdefault("body", m.get("body", b""))

            await app({"type": "http", "method": line[0], "path": line[1], "headers": headers}, receive, send)
            data = out.get("body", b"")
            w.write(f"HTTP/1.1 {out['status']} X\r\nContent-Type: application/json\r\nContent-Length: {len(data)}\r\nConnection: close\r\n\r\n".encode() + data)
            await w.drain()
        finally:
            w.close()

    s = await asyncio.start_server(conn, "127.0.0.1", 0, ssl=tls)
    SERVERS.append(s)
    port = s.sockets[0].getsockname()[1]
    return (f"https://localhost:{port}" if tls else f"http://127.0.0.1:{port}"), s


def plain(fn: Any) -> Any:
    """An ASGI app from fn(path, message) -> (status, body), for remote ends written by hand."""

    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        m = json.loads((await receive())["body"])
        status, body = await fn(scope["path"], m)
        await send({"type": "http.response.start", "status": status, "headers": []})
        await send({"type": "http.response.body", "body": json.dumps(body).encode()})

    return app


class CallbackServer:
    """The callback's server, in front of whichever process is "running" now:
    a callback may reach a process other than the one that called."""

    def __init__(self) -> None:
        self.current: Kairo | None = None

    async def start(self) -> str:
        async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
            assert self.current is not None
            await self.current.asgi_app()(scope, receive, send)

        url, self.server = await serve(app)
        return url + "/kairo/callback"


@unittest.skipUnless(HAS_GO and HAS_WASMTIME, "go or wasmtime not found")
class HttpActionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.dir = tempfile.mkdtemp(prefix="kairo-sdk-py-http-")
        cls.wasm = build_wasm(cls.dir)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.dir, ignore_errors=True)

    async def embedded(self, name: str) -> EmbeddedBackend:
        return await EmbeddedBackend.open(SQLiteStore(os.path.join(self.dir, name)), wasm=self.wasm)

    def test_answers_at_once(self) -> None:
        async def main() -> None:
            runs = []
            remote = Kairo(secret=SECRET)

            @remote.action("shout")
            def shout(i, ctx):
                runs.append(i)
                return i["s"].upper()

            url, _ = await serve(remote.asgi_app())
            cb = CallbackServer()
            k = Kairo(backend=await self.embedded("sync.db"), secret=SECRET, callback_url=await cb.start())
            k.action("shout", url=url + "/kairo/action")(lambda i, ctx: self.fail("runs at the URL"))

            @k.workflow("w")
            async def w(ctx, s):
                return await ctx.call("shout", {"s": s})

            await k.start()
            cb.current = k
            self.assertEqual(await k.run("w", "hi", id="sync-1"), "HI")
            self.assertEqual(len(runs), 1)
            await k.close()

        run(main)

    def test_answers_later_to_another_process(self) -> None:
        async def main() -> None:
            runs = []
            released = asyncio.Event()
            remote = Kairo(secret=SECRET)

            @remote.action("render", async_=True)
            async def render(i, ctx):
                runs.append(i)
                await released.wait()
                return {"frames": i["n"] * 2}

            url, _ = await serve(remote.asgi_app())
            cb = CallbackServer()
            cb_url = await cb.start()
            path = "async.db"

            async def invocation() -> Kairo:
                k = Kairo(backend=await self.embedded(path), mode="suspend", secret=SECRET, callback_url=cb_url)
                k.action("render", url=url + "/kairo/action", async_=True)(lambda i, ctx: self.fail("runs at the URL"))

                @k.workflow("w")
                async def w(ctx, n):
                    return await ctx.call("render", {"n": n})

                await k.start()
                cb.current = k
                return k

            # 1: calls the action, which accepts it; this process ends.
            k = await invocation()
            with self.assertRaises(Suspended):
                await k.run("w", 21, id="async-1")
            self.assertEqual(len(runs), 1)
            await k.close()

            # 2: another process takes the callback, and the workflow ends.
            k = await invocation()
            released.set()
            for i in range(400):
                try:
                    self.assertEqual(await k.run("w", 21, id="async-1"), {"frames": 42})
                    break
                except Suspended:
                    if i == 399:
                        raise
                    await asyncio.sleep(0.025)
            self.assertEqual(len(runs), 1, "the action ran once")
            await k.close()

        run(main)

    def test_callback_never_comes(self) -> None:
        """Unprotected retries; real stops for review, and is not called again."""

        async def main() -> None:
            calls = {"idem": 0, "real": 0}

            async def remote(_path: str, m: dict[str, Any]) -> tuple[int, Any]:
                calls[m["action"]] += 1
                if m["action"] == "idem" and calls["idem"] > 1:
                    return 200, {"output": f"attempt {m['attempt']}"}
                return 202, {"accepted": True}  # accepted, and forgotten

            url, _ = await serve(plain(remote))
            cb = CallbackServer()
            cb_url = await cb.start()

            async def invocation() -> Kairo:
                k = Kairo(backend=await self.embedded("expire.db"), mode="suspend", secret=SECRET, callback_url=cb_url)
                k.action("idem", effect="unprotected", url=url, timeout="200ms")(lambda i, ctx: None)
                k.action("real", url=url, timeout="200ms")(lambda i, ctx: None)

                @k.workflow("w")
                async def w(ctx, _):
                    a = await ctx.call("idem", {})
                    try:
                        await ctx.call("real", {})
                        return {"a": a, "real": "ok"}
                    except Suspended:
                        raise
                    except Exception as e:
                        return {"a": a, "real": str(e)}

                await k.start()
                cb.current = k
                return k

            k = await invocation()
            with self.assertRaises(Suspended):
                await k.run("w", None, id="expire-1")
            await k.close()
            # The scheduler's ticks, in new processes: leases expire, the
            # unprotected step is retried (after its backoff), the real one stops.
            out = None
            for _ in range(40):
                await asyncio.sleep(0.1)
                k = await invocation()
                await k.tick()
                try:
                    out = await k.run("w", None, id="expire-1")
                except Suspended:
                    pass
                await k.close()
                if out is not None:
                    break
            self.assertIsNotNone(out, "the workflow ended")
            # Retried at least once; more if a process closed while a retry
            # it had started was still out (its outcome is dropped, as a
            # process that stops drops it, and the lease expires again).
            self.assertGreaterEqual(calls["idem"], 2)
            self.assertEqual(out["a"], f"attempt {calls['idem']}")
            self.assertIn("blocked", out["real"])
            self.assertEqual(calls["real"], 1, "the real action is not called again")

        run(main)

    def test_status_codes(self) -> None:
        """4xx fails the step; 5xx is unknown: unprotected retries, real stops for review."""

        async def main() -> None:
            calls = {"bad": 0, "flaky": 0, "down": 0}

            async def remote(_path: str, m: dict[str, Any]) -> tuple[int, Any]:
                calls[m["action"]] += 1
                if m["action"] == "bad":
                    return 422, {"error": "no such thing"}
                if m["action"] == "flaky":
                    return (503, {}) if calls["flaky"] == 1 else (200, {"output": "ok"})
                return 500, {}

            url, _ = await serve(plain(remote))
            cb = CallbackServer()
            k = Kairo(backend=await self.embedded("status.db"), secret=SECRET, callback_url=await cb.start())
            for name, effect in (("bad", "unprotected"), ("flaky", "unprotected"), ("down", "real")):
                k.action(name, effect=effect, url=url)(lambda i, ctx: None)

            @k.workflow("w")
            async def w(ctx, name):
                try:
                    return await ctx.call(name, {})
                except Exception as e:
                    return str(e)

            await k.start()
            cb.current = k
            self.assertRegex(await k.run("w", "bad", id="st-bad"), r"failed.*422")
            self.assertEqual(calls["bad"], 1, "a 4xx is not retried")
            self.assertEqual(await k.run("w", "flaky", id="st-flaky"), "ok")
            self.assertEqual(calls["flaky"], 2)
            self.assertIn("blocked", await k.run("w", "down", id="st-down"))
            self.assertEqual(calls["down"], 1)
            await k.close()

        run(main)

    def test_signatures(self) -> None:
        async def main() -> None:
            runs = []
            k = Kairo(secret=SECRET)
            k.action("x")(lambda i, ctx: runs.append(i) or 1)
            base, _ = await serve(k.asgi_app())
            body = json.dumps({"run_id": "r", "act": 1, "attempt": 1, "action": "x", "input": None}).encode()
            for path in ("/kairo/action", "/kairo/callback"):
                for sig in (None, sign("other", body), sign(SECRET, body + b" "), sign(SECRET, body, int(time.time() * 1000) - 600_000)):
                    r = await asyncio.to_thread(post, base + path, body, headers={"Kairo-Signature": sig} if sig else {})
                    self.assertEqual(r.status, 401, f"{path} with {sig}")
            self.assertEqual(runs, [])
            r = await asyncio.to_thread(post, base + "/kairo/action", body, headers={"Kairo-Signature": sign(SECRET, body)})
            self.assertEqual((r.status, json.loads(r.body)), (200, {"output": 1}))

        run(main)

    def test_serving_side_sends_retryable_and_unknown(self) -> None:
        """A handler's RetryableError is sent as retryable; its
        UnknownOutcomeError is answered with 502, which the caller takes as
        unknown."""

        def raises(e: Exception):
            def handler(_, ctx):
                raise e

            return handler

        async def main() -> None:
            k = Kairo(secret=SECRET)
            k.action("busy", effect="unprotected")(raises(RetryableError("busy")))
            k.action("lost")(raises(UnknownOutcomeError("connection reset")))
            k.action("bad")(raises(OSError("no such file")))
            base, _ = await serve(k.asgi_app())

            async def call(action: str) -> tuple[int, Any]:
                body = json.dumps({"run_id": "r", "act": 1, "attempt": 1, "action": action, "input": None}).encode()
                r = await asyncio.to_thread(post, base + "/kairo/action", body, headers={"Kairo-Signature": sign(SECRET, body)})
                return r.status, json.loads(r.body)

            self.assertEqual(await call("busy"), (200, {"error": "RetryableError: busy", "retryable": True, "error_type": "RetryableError"}))
            self.assertEqual(await call("lost"), (502, {"error": "UnknownOutcomeError: connection reset", "error_type": "UnknownOutcomeError"}))
            self.assertEqual(await call("bad"), (200, {"error": "OSError: no such file", "error_type": "OSError"}))

        run(main)

    def test_plain_http_only_here(self) -> None:
        with self.assertRaisesRegex(ValueError, "plain http"):
            check_url("http://example.com/a")
        with self.assertRaisesRegex(ValueError, "only http"):
            check_url("ftp://localhost/a")
        for u in ("https://example.com/a", "http://localhost:1/a", "http://127.0.0.1/a", "http://[::1]:8/a"):
            check_url(u)
        check_url("http://10.0.0.5/a", True)

        async def main() -> None:
            k = Kairo(backend=await self.embedded("urls.db"), secret=SECRET, callback_url="http://10.0.0.5/cb")
            with self.assertRaisesRegex(ValueError, "plain http"):
                k.action("a", url="http://10.0.0.5/a")
            k.action("a", url="http://10.0.0.5/a", allow_insecure=True)(lambda i, ctx: None)
            with self.assertRaisesRegex(ValueError, "plain http"):
                await k.start()  # the callback URL is checked too
            await k.close()

        run(main)

    @unittest.skipUnless(HAS_OPENSSL, "openssl not found")
    def test_https_with_our_own_certificate(self) -> None:
        key, cert = os.path.join(self.dir, "key.pem"), os.path.join(self.dir, "cert.pem")
        subprocess.run(
            ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", key, "-out", cert, "-days", "1",
             "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost"],
            check=True, capture_output=True,
        )
        tls = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        tls.load_cert_chain(cert, key)
        with open(cert) as f:
            ca = f.read()

        async def main() -> None:
            remote = Kairo(secret=SECRET)
            remote.action("shout")(lambda s, ctx: s.upper())
            url, _ = await serve(remote.asgi_app(), tls)
            url += "/kairo/action"
            with self.assertRaises(OSError, msg="a certificate not trusted is refused"):
                await asyncio.to_thread(post, url, b"{}")

            cb = CallbackServer()
            k = Kairo(backend=await self.embedded("tls.db"), secret=SECRET, callback_url=await cb.start(), ca=ca)
            k.action("shout", effect="unprotected", url=url)(lambda i, ctx: None)

            @k.workflow("w")
            async def w(ctx, s):
                return await ctx.call("shout", s)

            await k.start()
            cb.current = k
            self.assertEqual(await k.run("w", "hi", id="tls-1"), "HI")
            await k.close()

        run(main)


if __name__ == "__main__":
    unittest.main()
