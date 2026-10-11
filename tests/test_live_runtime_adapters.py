from __future__ import annotations

import json
import unittest

from magic_glass_box.executor import SandboxPlan
from scripts.check_live_runtime_adapters import (
    ANTHROPIC_MODEL,
    CODEX_MODEL,
    PROOF_TEXT,
    ProviderObservation,
    _anthropic_sse,
    _assert_observations,
    _assert_sandbox_plans,
    _openai_sse,
    _provider_policy,
)


def _events(raw: bytes) -> list[tuple[str | None, object]]:
    decoded: list[tuple[str | None, object]] = []
    for frame in raw.decode("utf-8").strip().split("\n\n"):
        event_name = None
        data = None
        for line in frame.splitlines():
            if line.startswith("event: "):
                event_name = line.removeprefix("event: ")
            elif line.startswith("data: "):
                value = line.removeprefix("data: ")
                data = value if value == "[DONE]" else json.loads(value)
        decoded.append((event_name, data))
    return decoded


class LiveRuntimeProofContractTests(unittest.TestCase):
    def test_openai_fixture_is_a_complete_stream_for_the_pinned_model(self) -> None:
        events = _events(_openai_sse(CODEX_MODEL))

        self.assertEqual(events[0][0], "response.created")
        self.assertEqual(events[-2][0], "response.completed")
        self.assertEqual(events[-1], (None, "[DONE]"))
        completed = events[-2][1]
        self.assertIsInstance(completed, dict)
        response = completed["response"]  # type: ignore[index]
        self.assertEqual(response["model"], CODEX_MODEL)
        self.assertEqual(response["output"][0]["content"][0]["text"], PROOF_TEXT)

    def test_anthropic_fixture_is_a_complete_stream_for_the_pinned_model(self) -> None:
        events = _events(_anthropic_sse(ANTHROPIC_MODEL))

        self.assertEqual(
            [name for name, _ in events],
            [
                "message_start",
                "content_block_start",
                "content_block_delta",
                "content_block_stop",
                "message_delta",
                "message_stop",
            ],
        )
        self.assertEqual(events[0][1]["message"]["model"], ANTHROPIC_MODEL)  # type: ignore[index]
        self.assertEqual(events[2][1]["delta"]["text"], PROOF_TEXT)  # type: ignore[index]

    def test_fixture_policies_pin_vendor_routes_models_and_credential_headers(
        self,
    ) -> None:
        openai = _provider_policy(8765, "codex")
        anthropic = _provider_policy(8765, "claude-code")

        self.assertEqual(openai.destination, "http://127.0.0.1:8765/v1/responses")
        self.assertEqual(openai.adapter, "openai_responses")
        self.assertEqual(openai.model, CODEX_MODEL)
        self.assertEqual(openai.credential_header, "authorization")
        self.assertEqual(openai.credential_prefix, "Bearer ")
        self.assertEqual(anthropic.destination, "http://127.0.0.1:8765/v1/messages")
        self.assertEqual(anthropic.adapter, "anthropic_messages")
        self.assertEqual(anthropic.model, ANTHROPIC_MODEL)
        self.assertEqual(anthropic.credential_header, "x-api-key")
        self.assertEqual(anthropic.credential_prefix, "")

    def test_proof_assertions_reject_wrong_upstream_or_sandbox_evidence(self) -> None:
        valid_observations = (
            ProviderObservation("openai-responses", True, CODEX_MODEL, True),
            ProviderObservation("anthropic-messages", True, ANTHROPIC_MODEL, True),
        )
        _assert_observations(valid_observations)
        with self.assertRaisesRegex(RuntimeError, "credential"):
            _assert_observations(
                (
                    ProviderObservation("openai-responses", False, CODEX_MODEL, True),
                    valid_observations[1],
                )
            )

        plans = (
            SandboxPlan(
                "run_" + "1" * 32,
                "magic-glass-box-" + "1" * 32,
                "sha256:" + "a" * 64,
                (
                    "container",
                    "create",
                    "--network",
                    "none",
                    "--env",
                    "MAGIC_GLASS_BOX_PROVIDER_TOKEN=local-gateway-not-a-secret",
                ),
            ),
            SandboxPlan(
                "run_" + "2" * 32,
                "magic-glass-box-" + "2" * 32,
                "sha256:" + "a" * 64,
                ("container", "create", "--network", "none"),
            ),
        )
        _assert_sandbox_plans(plans, ("openai-private", "anthropic-private"))
        escaped = SandboxPlan(
            plans[0].run_id,
            plans[0].container_name,
            plans[0].image_id,
            ("container", "create", "--network", "bridge"),
        )
        with self.assertRaisesRegex(RuntimeError, "networkless"):
            _assert_sandbox_plans((escaped, plans[1]), ())


if __name__ == "__main__":
    unittest.main()
