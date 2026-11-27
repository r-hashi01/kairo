"""The pure core of kairo as a WASM module (cmd/kairo-wasm, ADR 0051).

Plans are compiled once, then events applied to a run's state given as
bytes. No I/O happens here; the embedded runtime reads and writes the state.
Needs wasmtime (``pip install "kairo-sdk[embedded]"``).
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import struct
import tempfile
import threading
from pathlib import Path
from typing import Any

#: The ABI version this SDK speaks (wasmcore.ABIVersion).
ABI_VERSION = 1

DEFAULT_WASM = Path(__file__).parent / "wasm" / "kairo.wasm"


class CoreError(Exception):
    pass


# Compiling kairo.wasm is most of what opening a Core costs (about 360 ms
# with wasmtime, against a few ms to instantiate it): a serverless function
# that opens one for each call pays it each time (issue #107). A module is
# compiled once a process, and its machine code kept in a cache directory
# that later processes read instead (Module.deserialize_file).
_lock = threading.Lock()
_engine: Any = None
_modules: dict[bytes, Any] = {}


def _compiled(data: bytes) -> tuple[Any, Any]:
    """The engine, and kairo.wasm (data) compiled for it."""
    global _engine
    from wasmtime import Engine

    key = hashlib.sha256(data).digest()
    with _lock:
        if _engine is None:
            _engine = Engine()
        module = _modules.get(key)
        if module is None:
            module = _modules[key] = _load(_engine, data, key)
        return _engine, module


def _compile(engine: Any, data: bytes) -> Any:
    from wasmtime import Module

    return Module(engine, data)


def cache_dir() -> Path | None:
    """Where compiled kairo.wasm is kept: KAIRO_WASM_CACHE, or the user's
    cache directory (XDG_CACHE_HOME, else ~/.cache) under kairo-sdk;
    None when KAIRO_WASM_CACHE is "off". A serverless function points it at
    a directory shipped with its code, compiled there when it is built."""
    env = os.environ.get("KAIRO_WASM_CACHE")
    if env == "off":
        return None
    if env:
        return Path(env)
    base = os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache"
    return Path(base) / "kairo-sdk"


def _trusted(d: Path) -> bool:
    # Machine code is run as it is read: only from a directory nobody else
    # can write to.
    if not hasattr(os, "getuid"):
        return True
    st = d.stat()
    return st.st_uid == os.getuid() and st.st_mode & 0o022 == 0


#: How many compiled cores a cache directory keeps (the latest written):
#: one for each kairo-sdk and wasmtime in use, not one for every build.
CACHE_KEEP = 8


def _prune(d: Path) -> None:
    kept = sorted(d.glob("*.cwasm"), key=lambda p: p.stat().st_mtime, reverse=True)
    for old in kept[CACHE_KEEP:]:
        old.unlink(missing_ok=True)


def _load(engine: Any, data: bytes, key: bytes) -> Any:
    """kairo.wasm compiled for engine: from the cache directory if it is
    there for this wasmtime and machine, else compiled and kept there. A
    cache that cannot be read or written is passed over."""
    from importlib.metadata import version

    from wasmtime import Module

    try:
        d = cache_dir()
    except Exception:  # no home directory
        d = None
    if d is None:
        return _compile(engine, data)
    try:
        name = f"{key.hex()[:32]}-wasmtime{version('wasmtime')}-{platform.system()}-{platform.machine()}.cwasm".lower()
    except Exception:  # pragma: no cover - wasmtime installed without metadata
        return _compile(engine, data)
    path = d / name
    try:
        if path.is_file() and _trusted(d):
            return Module.deserialize_file(engine, str(path))
    except Exception:
        pass  # not this wasmtime's after all: compiled again below
    module = _compile(engine, data)
    try:
        d.mkdir(mode=0o700, parents=True, exist_ok=True)
        if _trusted(d):
            fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
            try:
                with os.fdopen(fd, "wb") as f:
                    f.write(module.serialize())
                os.replace(tmp, path)
            except BaseException:
                os.unlink(tmp)
                raise
            _prune(d)
    except Exception:
        pass  # a read-only place (a serverless function's code): not kept
    return module


class Core:
    def __init__(self, wasm: str | Path | bytes | None = None) -> None:
        try:
            from wasmtime import Engine, Linker, Module, Store, WasiConfig
        except ImportError as e:  # pragma: no cover - depends on the install
            raise CoreError("the embedded runtime needs wasmtime: pip install 'kairo-sdk[embedded]'") from e
        data = wasm if isinstance(wasm, bytes) else Path(wasm or DEFAULT_WASM).read_bytes()
        engine, module = _compiled(data)
        self._store = Store(engine)
        self._store.set_wasi(WasiConfig())
        linker = Linker(engine)
        linker.define_wasi()
        ex = linker.instantiate(self._store, module).exports(self._store)
        ex["_initialize"](self._store)
        self._mem = ex["memory"]
        self._alloc = ex["kairo_alloc"]
        self._free = ex["kairo_free"]
        self._result = ex["kairo_result"]
        self._register = ex["kairo_register"]
        self._compile = ex["kairo_compile"]
        self._apply = ex["kairo_apply"]
        self._inspect = ex["kairo_inspect"]
        v = ex["kairo_abi_version"](self._store)
        if v != ABI_VERSION:
            raise CoreError(f"kairo.wasm speaks ABI {v}, this SDK {ABI_VERSION}")

    def _put(self, b: bytes) -> tuple[int, int]:
        if not b:
            return 0, 0
        p = self._alloc(self._store, len(b))
        self._mem.write(self._store, b, p)
        return p, len(b)

    def _read(self, n: int) -> bytes:
        p = self._result(self._store)
        b = bytes(self._mem.read(self._store, p, p + abs(n))) if n else b""
        if n < 0:
            raise CoreError(b.decode())
        return b

    def _call(self, f: Any, *bufs: bytes) -> bytes:
        args: list[int] = []
        ptrs: list[int] = []
        for b in bufs:
            p, n = self._put(b)
            args += [p, n]
            if p:
                ptrs.append(p)
        try:
            return self._read(f(self._store, *args))
        finally:
            for p in ptrs:
                self._free(self._store, p)

    def register(self, specs: list[dict[str, Any]]) -> None:
        """Registers node specs (ir.NodeSpec)."""
        self._call(self._register, json.dumps(specs).encode())

    def compile(self, definition: dict[str, Any]) -> dict[str, Any]:
        """Compiles a workflow definition (ir.Definition): {plan, name, hash, has_real, effects,
        idempotent}."""
        return json.loads(self._call(self._compile, json.dumps(definition).encode()))

    def inspect(self, state: bytes) -> dict[str, Any]:
        """Reads a run's state (wasmcore.Inspection, ADR 0060): {intent_durable,
        review, intents}. review: the activations stopped for review; intents:
        those with a real attempt out and its intent durable (in order)."""
        return json.loads(self._call(self._inspect, state))

    def apply(self, plan: int, run_id: str, state: bytes, event: dict[str, Any], traced: bool = False) -> tuple[bytes, dict[str, Any]]:
        """Applies event (wasmcore.Event) to a run's state (empty for a new run)."""
        bufs = [run_id.encode(), state, json.dumps(event).encode()]
        args: list[int] = []
        ptrs: list[int] = []
        for b in bufs:
            p, n = self._put(b)
            args += [p, n]
            if p:
                ptrs.append(p)
        try:
            out = self._read(self._apply(self._store, plan, *args, 1 if traced else 0))
        finally:
            for p in ptrs:
                self._free(self._store, p)
        n = struct.unpack_from("<I", out, 0)[0]
        return out[4 : 4 + n], json.loads(out[4 + n :])
