from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import json
import os
import threading
from pathlib import Path

from kairo_sdk import HttpBackend, Kairo, Suspended


ACTION = "qualify.python.once"
WORKFLOW = "qualify.python"
VERSION = "qualification-v1"


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def marker(directory: str, run_id: str) -> str:
    return os.path.join(directory, hashlib.sha256(run_id.encode()).hexdigest() + ".done")


async def run_one(k: Kairo, tick, run_id: str) -> None:
    try:
        await k.run(WORKFLOW, run_id, id=run_id)
    except Suspended:
        pass
    else:
        raise RuntimeError(f"{run_id}: initial run did not suspend")
    while True:
        await tick()
        children = await k.children(run_id)
        if any(str(row.get("plan", "")).startswith("kairo.wait/continue") and row.get("status") not in ("completed", "failed", "cancelled") for row in children):
            await k.signal(run_id, "continue", run_id)
            break
        await asyncio.sleep(0.005)
    while True:
        try:
            result = await k.result(run_id)
            if result != run_id:
                raise RuntimeError(f"{run_id}: result {result!r}")
            return
        except Suspended:
            await tick()


async def main() -> None:
    url = required("KAIRO_QUALIFY_URL")
    socket = required("KAIRO_QUALIFY_SOCKET")
    markers = required("KAIRO_QUALIFY_MARKERS")
    prefix = required("KAIRO_QUALIFY_PREFIX")
    runs = int(required("KAIRO_QUALIFY_RUNS"))
    inflight = min(int(required("KAIRO_QUALIFY_INFLIGHT")), runs)
    timeout_seconds = int(required("KAIRO_QUALIFY_TIMEOUT_SECONDS"))
    barrier = os.environ.get("KAIRO_QUALIFY_BARRIER")
    driver = os.environ.get("KAIRO_QUALIFY_DRIVER")
    if runs < 1 or inflight < 1 or timeout_seconds < 1:
        raise RuntimeError("qualification counts and timeout must be positive")
    backend = HttpBackend(url, worker=socket, concurrency=64, lease_ms=1000)
    output_lock = threading.Lock()

    def observe(o) -> None:
        if o.kind not in ("step.started", "step.finished"):
            return
        row = {"phase": "worker", **dataclasses.asdict(o), "at_unix_ms": o.at, "duration_ms": (o.duration or 0) * 1000}
        with output_lock:
            print(json.dumps(row, separators=(",", ":")), flush=True)

    k = Kairo(backend=backend, mode="suspend", wake=lambda _at: asyncio.sleep(0), observe=observe)

    @k.action(ACTION)
    def once(run_id, _ctx):
        path = marker(markers, run_id)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        with os.fdopen(fd, "w") as out:
            out.write(run_id + "\n")
        return run_id

    @k.workflow(WORKFLOW, version=VERSION)
    async def qualify(ctx, run_id):
        await ctx.call(ACTION, run_id)
        await ctx.sleep(0.025)
        return await ctx.wait_for("continue")

    await k.start()
    async def tick() -> None:
        began = asyncio.get_running_loop().time()
        try:
            await k.tick()
        finally:
            with output_lock:
                print(json.dumps({"phase": "claim_drive", "duration_ms": (asyncio.get_running_loop().time() - began) * 1000}, separators=(",", ":")), flush=True)

    queue: asyncio.Queue[str] = asyncio.Queue()
    for i in range(runs):
        queue.put_nowait(f"qualify-python-{prefix}-{i:06d}")

    async def worker() -> None:
        while True:
            try:
                run_id = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                await run_one(k, tick, run_id)
            finally:
                queue.task_done()

    try:
        async with asyncio.timeout(timeout_seconds):
            await asyncio.gather(*(worker() for _ in range(inflight)))
            if barrier and driver:
                Path(barrier, driver + ".done").touch()
                while not Path(barrier, "release").exists():
                    await asyncio.sleep(0.05)
    finally:
        await k.close()
    print(f"python driver {prefix}: {runs} workflows completed")


if __name__ == "__main__":
    asyncio.run(main())
