from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from scripts.check_live_krab_transport import main


class LiveKrabTransportProofTests(unittest.TestCase):
    def test_missing_sister_source_fails_before_starting_a_server(self) -> None:
        missing = Path(__file__).parent / "missing-imitation-krab-source"
        with (
            mock.patch(
                "sys.argv",
                ["check_live_krab_transport.py", "--krab-source", str(missing)],
            ),
            self.assertRaisesRegex(RuntimeError, "imitation-krab source not found"),
        ):
            main()

    def test_live_http_transport_proves_idempotency_and_privacy(self) -> None:
        source = Path(__file__).resolve().parents[2] / "imitation-krab" / "src"
        if not (source / "imitation_krab" / "__init__.py").is_file():
            self.skipTest("the optional imitation-krab sister checkout is unavailable")

        stdout = io.StringIO()
        with (
            mock.patch(
                "sys.argv",
                ["check_live_krab_transport.py", "--krab-source", str(source)],
            ),
            redirect_stdout(stdout),
        ):
            status = main()

        self.assertEqual(status, 0)
        evidence = json.loads(stdout.getvalue())
        self.assertEqual(evidence["idempotency_replay"], "same_item")
        self.assertEqual(evidence["reminder_idempotency_replay"], "same_event")
        self.assertFalse(evidence["private_content_persisted_by_mgb"])
        self.assertEqual(evidence["control_room_role"], "coordinator")
        self.assertEqual(evidence["answer_surface"], "cli")
        self.assertEqual(evidence["interaction_state"], "answered")
        self.assertEqual(evidence["run_state"], "queued")


if __name__ == "__main__":
    unittest.main()
