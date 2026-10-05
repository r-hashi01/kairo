"""The worker protocol against a fake runtime on a socket pair."""

from __future__ import annotations

import base64
import socket
import threading
import unittest

from kairo_worker import MsgType, Result, Task, TaskContext, Worker, read_frame, write_frame


class FakeRuntime:
    def __init__(self) -> None:
        self.server, client = socket.socketpair()
        self.r = self.server.makefile("rb")
        self.w = self.server.makefile("wb")
        self.client = client

    def send(self, t: MsgType, body: dict) -> None:
        write_frame(self.w, t, body)
        self.w.flush()

    def recv(self) -> tuple[int, dict]:
        return read_frame(self.r)


def serve(worker: Worker, rt: FakeRuntime) -> threading.Thread:
    # Worker.run connects by address; serve the socket pair's end instead.
    def run() -> None:
        worker._run_on(rt.client.makefile("rb"), rt.client.makefile("wb"))

    th = threading.Thread(target=run, daemon=True)
    th.start()
    return th


class WorkerTest(unittest.TestCase):
    def test_task_chunks_and_result(self) -> None:
        def handler(task: Task, ctx: TaskContext) -> Result:
            ctx.emit("he")
            ctx.emit(b"llo")
            return Result(output={"echo": task.input}, meta={"usage": 3}, tokens=3)

        rt = FakeRuntime()
        serve(Worker("w", ["x"], handler, 2), rt)
        t, hello = rt.recv()
        self.assertEqual(t, MsgType.HELLO)
        self.assertEqual(hello, {"worker": "w", "actions": ["x"], "credit": 2})
        rt.send(MsgType.TASK, {"seq": 7, "action": "x", "step_id": "n[1]", "input": {"a": 1}})
        chunks = []
        while True:
            t, body = rt.recv()
            if t == MsgType.CHUNK:
                chunks.append(base64.b64decode(body["data"]))
                continue
            self.assertEqual(t, MsgType.RESULT)
            self.assertEqual(body, {"seq": 7, "output": {"echo": {"a": 1}}, "tokens": 3, "meta": {"usage": 3}})
            break
        self.assertEqual(b"".join(chunks), b"hello")

    def test_cancel_reaches_the_handler(self) -> None:
        started = threading.Event()

        def handler(task: Task, ctx: TaskContext) -> Result:
            started.set()
            if not ctx.cancelled.wait(5):
                return Result(output="not cancelled")
            return Result(error="cancelled", retryable=True)

        rt = FakeRuntime()
        serve(Worker("w", ["x"], handler), rt)
        rt.recv()
        rt.send(MsgType.TASK, {"seq": 1, "action": "x"})
        started.wait(5)
        rt.send(MsgType.CANCEL, {"seq": 1})
        rt.send(99, {"unknown": True})  # skipped
        t, body = rt.recv()
        self.assertEqual((t, body.get("error")), (MsgType.RESULT, "cancelled"))

    def test_handler_exception_still_answers(self) -> None:
        def handler(task: Task, ctx: TaskContext) -> Result:
            raise ValueError("bad")

        rt = FakeRuntime()
        serve(Worker("w", ["x"], handler), rt)
        rt.recv()
        rt.send(MsgType.TASK, {"seq": 2, "action": "x"})
        t, body = rt.recv()
        self.assertEqual(t, MsgType.RESULT)
        self.assertEqual(body["error_type"], "ValueError")


if __name__ == "__main__":
    unittest.main()
