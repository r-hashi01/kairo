"""Actions called over HTTP(S) (ADR 0052): signing, posting, and checking
where a request may go. No dependency: urllib, ssl and hmac.

The same contract as the TypeScript SDK's (sdk/ts/src/http.ts)."""

from __future__ import annotations

import hashlib
import hmac
import ssl
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from urllib.parse import urlsplit

#: How old a signed request may be (ms).
SIGNATURE_TOLERANCE = 5 * 60 * 1000

_LOCAL = {"localhost", "127.0.0.1", "::1"}


class SignatureError(Exception):
    pass


def _mac(secret: str, t: int, body: bytes) -> str:
    return hmac.new(secret.encode(), str(t).encode() + b"." + body, hashlib.sha256).hexdigest()


def sign(secret: str, body: bytes, t: int | None = None) -> str:
    """The Kairo-Signature header of body at time t (unix ms):
    "t=<t>,v1=<hex HMAC-SHA256(secret, t + '.' + body)>"."""
    t = int(time.time() * 1000) if t is None else t
    return f"t={t},v1={_mac(secret, t, body)}"


def verify(secret: str, body: bytes, header: str | None, now: int | None = None) -> None:
    """Raises SignatureError unless header signs body with secret, recently."""
    parts = dict(p.split("=", 1) for p in (header or "").split(",") if "=" in p)
    try:
        t = int(parts.get("t", ""))
    except ValueError:
        raise SignatureError("missing signature") from None
    if not parts.get("v1"):
        raise SignatureError("missing signature")
    now = int(time.time() * 1000) if now is None else now
    if abs(now - t) > SIGNATURE_TOLERANCE:
        raise SignatureError("signature too old")
    if not hmac.compare_digest(parts["v1"], _mac(secret, t, body)):
        raise SignatureError("bad signature")


def check_url(url: str, allow_insecure: bool = False) -> None:
    """Checks where a request may go: https, or http to this machine, or
    http when allow_insecure says so (ADR 0052)."""
    u = urlsplit(url)
    if u.scheme == "https" and u.hostname:
        return
    if u.scheme == "http" and u.hostname:
        if u.hostname in _LOCAL or allow_insecure:
            return
        raise ValueError(f"{url}: plain http is only for this machine; use https, or allow_insecure")
    raise ValueError(f"{url}: only http(s)")


@dataclass
class Response:
    status: int
    body: bytes


def post(url: str, body: bytes, *, ca: str | None = None, timeout: float = 30, headers: dict[str, str] | None = None) -> Response:
    """POSTs a JSON body (blocking); raises OSError on a network error or a
    timeout. ca: certificates to trust (PEM), besides the system's."""
    req = urllib.request.Request(url, data=body, method="POST", headers={"Content-Type": "application/json", **(headers or {})})
    ctx = None
    if url.startswith("https:"):
        ctx = ssl.create_default_context()
        if ca:
            ctx.load_verify_locations(cadata=ca)
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            return Response(r.status, r.read())
    except urllib.error.HTTPError as e:
        with e:
            return Response(e.code, e.read())
