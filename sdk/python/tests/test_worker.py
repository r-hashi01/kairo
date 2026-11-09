"""The worker protocol against a fake runtime on a socket pair."""

from __future__ import annotations

import base64
import socket
import threading
import unittest

from kairo_sdk import CAPABILITY_GRACEFUL_DRAIN, DrainUnsupportedError, MsgType, Result, Task, TaskContext, Worker, read_frame, write_frame


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
        self.assertEqual(hello, {"worker": "w", "actions": ["x"], "credit": 2, "capabilities": [CAPABILITY_GRACEFUL_DRAIN]})
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

    def test_hello_carries_the_token(self) -> None:
        rt = FakeRuntime()
        serve(Worker("w", ["x"], lambda t, c: Result(), 1, token="s3"), rt)
        _, hello = rt.recv()
        self.assertEqual(hello["token"], "s3")

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

    def test_drain_waits_until_results_are_accepted(self) -> None:
        started = threading.Event()
        release = threading.Event()

        def handler(task: Task, ctx: TaskContext) -> Result:
            started.set()
            release.wait(5)
            return Result(output="done")

        rt = FakeRuntime()
        worker = Worker("w", ["x"], handler, 1)
        serve(worker, rt)
        t, hello = rt.recv()
        self.assertEqual(t, MsgType.HELLO)
        self.assertIn(CAPABILITY_GRACEFUL_DRAIN, hello["capabilities"])
        rt.send(MsgType.HELLO, {"capabilities": [CAPABILITY_GRACEFUL_DRAIN]})
        rt.send(MsgType.TASK, {"seq": 3, "action": "x"})
        self.assertTrue(started.wait(5))
        done = threading.Event()
        threading.Thread(target=lambda: (worker.drain(), done.set()), daemon=True).start()
        t, body = rt.recv()
        self.assertEqual((t, body), (MsgType.CREDIT, {"n": -1}))
        rt.send(MsgType.CREDIT, {"n": 0})
        self.assertFalse(done.wait(0.05))
        release.set()
        t, body = rt.recv()
        self.assertEqual((t, body.get("output")), (MsgType.RESULT, "done"))
        self.assertFalse(done.wait(0.05))
        rt.send(MsgType.CREDIT, {"n": 1})
        self.assertTrue(done.wait(5))

    def test_drain_fails_without_negotiated_capability(self) -> None:
        rt = FakeRuntime()
        worker = Worker("w", ["x"], lambda _t, _c: Result(), 1)
        serve(worker, rt)
        rt.recv()
        rt.send(MsgType.CANCEL, {"seq": 4})
        with self.assertRaises(DrainUnsupportedError):
            worker.drain()
        rt.server.settimeout(0.05)
        with self.assertRaises(socket.timeout):
            rt.server.recv(1)


if __name__ == "__main__":
    unittest.main()
