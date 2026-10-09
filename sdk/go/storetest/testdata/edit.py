"""The workflow of xlang_test.go, in Python: the same actions, the same
calls in the same order, so the same call ids and plans.

    python edit.py <sdk/python> <kairo.wasm> <db> start|finish <id>
"""

import asyncio
import json
import sys

sdk, wasm, db, mode, id = sys.argv[1:6]
sys.path.insert(0, sdk)

from kairo_sdk import EmbeddedBackend, Kairo, SQLiteStore  # noqa: E402

runs = {"llm": 0, "write": 0}


def ask(q):
    # Written as Python writes it: 1.0 is a float here (1 in JSON).
    return {"q": q, "temperature": 1.0, "😀": 1, "｡": 2}


async def main():
    k = Kairo(backend=await EmbeddedBackend.open(SQLiteStore(db), wasm=wasm))

    @k.action("llm", effect="unprotected")
    def llm(p, ctx):
        runs["llm"] += 1
        return p["q"].upper()

    @k.action("write", effect="real")
    def write(p, ctx):
        runs["write"] += 1
        return f"wrote {p}"

    @k.workflow("edit")
    async def edit(ctx, files):
        a = await ctx.call("llm", ask(files[0]))
        b = await ctx.call("llm", ask(files[1]))
        wrote = await ctx.call("write", files[0])
        if mode == "start":
            await asyncio.Event().wait()  # as a process that stops here
        again = await ctx.call("llm", ask("done"))
        return {"answers": [a, b], "wrote": wrote, "again": again}

    await k.start()
    if mode == "start":
        task = asyncio.ensure_future(k.run("edit", ["a", "b"], id=id))
        while runs["write"] < 1:
            await asyncio.sleep(0.005)
        await k.close()
        task.cancel()
        print(json.dumps({"runs": runs}))
    else:
        out = await k.run("edit", ["a", "b"], id=id)
        await k.close()
        print(json.dumps({"runs": runs, "out": out}))


asyncio.run(main())
