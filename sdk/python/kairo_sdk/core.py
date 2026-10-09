"""The pure core of kairo as a WASM module (cmd/kairo-wasm, ADR 0051).

Plans are compiled once, then events applied to a run's state given as
bytes. No I/O happens here; the embedded runtime reads and writes the state.
Needs wasmtime (``pip install "kairo-sdk[embedded]"``).
"""

from __future__ import annotations

import json
import struct
from pathlib import Path
from typing import Any

#: The ABI version this SDK speaks (wasmcore.ABIVersion).
ABI_VERSION = 1

DEFAULT_WASM = Path(__file__).parent / "wasm" / "kairo.wasm"


class CoreError(Exception):
    pass


class Core:
    def __init__(self, wasm: str | Path | bytes | None = None) -> None:
        try:
            from wasmtime import Engine, Linker, Module, Store, WasiConfig
        except ImportError as e:  # pragma: no cover - depends on the install
            raise CoreError("the embedded runtime needs wasmtime: pip install 'kairo-sdk[embedded]'") from e
        engine = Engine()
        data = wasm if isinstance(wasm, bytes) else Path(wasm or DEFAULT_WASM).read_bytes()
        module = Module(engine, data)
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
