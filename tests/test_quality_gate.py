from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CHECKER = ROOT / "scripts" / "check_mutation_score.py"


class MutationScoreGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.stats = Path(self.temporary.name) / "stats.json"

    def run_checker(self, payload: object) -> subprocess.CompletedProcess[str]:
        self.stats.write_text(json.dumps(payload), encoding="utf-8")
        return subprocess.run(  # noqa: S603 - fixed interpreter and local script
            [
                sys.executable,
                str(CHECKER),
                "--stats",
                str(self.stats),
                "--minimum",
                "80",
            ],
            check=False,
            capture_output=True,
            text=True,
        )

    def test_accepts_score_at_floor(self) -> None:
        result = self.run_checker({"killed": 8, "total": 10})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("80.00%", result.stdout)

    def test_rejects_score_below_floor(self) -> None:
        result = self.run_checker({"killed": 7, "total": 10})
        self.assertEqual(result.returncode, 1)
        self.assertIn("below required 80.00%", result.stderr)

    def test_rejects_malformed_or_empty_stats(self) -> None:
        for payload in ({"killed": True, "total": 10}, {"killed": 0, "total": 0}):
            with self.subTest(payload=payload):
                result = self.run_checker(payload)
                self.assertEqual(result.returncode, 2)
                self.assertIn("invalid mutation stats", result.stderr)


if __name__ == "__main__":
    unittest.main()
