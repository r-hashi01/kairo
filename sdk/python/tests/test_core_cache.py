"""kairo.wasm is compiled once a process, and its machine code is kept in a
cache directory that later processes read instead of compiling it again
(issue #107). Needs go (to build kairo.wasm) and wasmtime."""

from __future__ import annotations

import os
import shutil
import stat
import tempfile
import unittest
from unittest import mock

from kairo_sdk import core

try:
    from .test_workflow import HAS_GO, HAS_WASMTIME, build_wasm
except ImportError:
    from test_workflow import HAS_GO, HAS_WASMTIME, build_wasm

PLAN = {"name": "p", "root": {"kind": "step", "id": "s", "action": "a"}}


@unittest.skipUnless(HAS_GO and HAS_WASMTIME, "go or wasmtime not found")
class CoreCacheTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.dir = tempfile.mkdtemp(prefix="kairo-core-cache-")
        cls.wasm = build_wasm(cls.dir)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.dir, ignore_errors=True)

    def setUp(self) -> None:
        self.cache = tempfile.mkdtemp(prefix="kairo-wasm-cache-")
        self.addCleanup(shutil.rmtree, self.cache, True)
        self.compiles = 0
        real = core._compile

        def counted(engine, data):
            self.compiles += 1
            return real(engine, data)

        patches = [mock.patch.object(core, "_compile", counted), mock.patch.dict(core._modules, clear=True)]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def open(self, cache: str) -> core.Core:
        with mock.patch.dict(os.environ, {"KAIRO_WASM_CACHE": cache}):
            c = core.Core(self.wasm)
        self.assertEqual(c.compile(PLAN)["name"], "p")
        return c

    def kept(self) -> list[str]:
        return [n for n in os.listdir(self.cache) if n.endswith(".cwasm")]

    def test_compiled_once_a_process(self) -> None:
        self.open("off")
        self.open("off")
        self.assertEqual(self.compiles, 1)

    def test_a_later_process_reads_what_was_kept(self) -> None:
        self.open(self.cache)
        self.assertEqual(len(self.kept()), 1)
        core._modules.clear()  # as a new process
        self.open(self.cache)
        self.assertEqual(self.compiles, 1)

    def test_a_directory_others_can_write_is_not_used(self) -> None:
        if not hasattr(os, "getuid"):
            self.skipTest("no owners and modes here")
        os.chmod(self.cache, 0o777)
        self.open(self.cache)
        core._modules.clear()
        self.open(self.cache)
        self.assertEqual(self.compiles, 2)
        self.assertEqual(self.kept(), [])

    def test_a_read_only_directory_is_read_not_written(self) -> None:
        if not hasattr(os, "getuid") or os.getuid() == 0:
            self.skipTest("root writes anywhere")
        os.chmod(self.cache, stat.S_IRUSR | stat.S_IXUSR)
        self.addCleanup(os.chmod, self.cache, 0o700)
        self.open(self.cache)
        self.assertEqual((self.compiles, self.kept()), (1, []))

    def test_only_the_latest_are_kept(self) -> None:
        for i in range(core.CACHE_KEEP + 2):
            with open(os.path.join(self.cache, f"old{i}.cwasm"), "wb") as f:
                f.write(b"x")
            os.utime(os.path.join(self.cache, f"old{i}.cwasm"), (i, i))
        self.open(self.cache)
        kept = self.kept()
        self.assertEqual(len(kept), core.CACHE_KEEP)
        self.assertNotIn("old0.cwasm", kept)
        self.assertTrue(any(not n.startswith("old") for n in kept))

    def test_what_another_wasmtime_kept_is_compiled_again(self) -> None:
        self.open(self.cache)
        (name,) = self.kept()
        with open(os.path.join(self.cache, name), "r+b") as f:
            f.write(b"not machine code")
        core._modules.clear()
        self.open(self.cache)
        self.assertEqual(self.compiles, 2)


if __name__ == "__main__":
    unittest.main()
