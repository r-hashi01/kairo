"""Call ids are the TypeScript and Go SDKs' (ADR 0049, 0058): the same
input gives the same canonical text and the same id in every SDK, so a
workflow begun in one is resumed by another without running its calls
again."""

from __future__ import annotations

import json
import os
import unittest

from kairo_sdk.workflow import _call_id, _canonical

CASES = os.path.join(os.path.dirname(__file__), "..", "..", "testdata", "callids.jsonl")


class CanonicalTest(unittest.TestCase):
    def test_call_ids_match_typescript(self) -> None:
        """sdk/testdata/callids.jsonl was made by the TypeScript SDK's
        canonical and callId, on inputs at the edges."""
        with open(CASES, encoding="utf-8") as f:
            cases = [json.loads(line) for line in f if line.strip()]
        self.assertGreaterEqual(len(cases), 19)
        for c in cases:
            with self.subTest(input=c["input"]):
                self.assertEqual(_canonical(c["input"]), c["canonical"])
                self.assertEqual(_call_id("wf-1", "kairo.call/llm", c["input"], 2), c["id"])

    def test_python_values_as_javascript_has_them(self) -> None:
        """Values Python writes otherwise than JSON.stringify: floats that
        are integers, exponents, -0, NaN, integers beyond 2**53, key order
        by UTF-16 code units, lone surrogates."""
        for v, want in [
            (1.0, "1"),
            (-2.0, "-2"),
            (1e16, "10000000000000000"),
            (1e21, "1e+21"),
            (1e-6, "0.000001"),
            (1e-7, "1e-7"),
            (-2.5e-9, "-2.5e-9"),
            (1.5e300, "1.5e+300"),
            (5e-324, "5e-324"),
            (-0.0, "0"),
            (float("nan"), "null"),
            (float("inf"), "null"),
            (2**53, "9007199254740992"),
            (2**60 + 1, "1152921504606847000"),
            ((1, 2.0), "[1,2]"),
            ({"｡": 1, "😀": 2, "a": 3, "B": 4}, '{"B":4,"a":3,"😀":2,"｡":1}'),
            ("\ud800", '"\\ud800"'),
            ("a b<>&\x01", '"a b<>&\\u0001"'),
            ({1: "x", True: "y"}, '{"1":"y"}'),
        ]:
            with self.subTest(v=v):
                self.assertEqual(_canonical(v), want)


if __name__ == "__main__":
    unittest.main()
