from __future__ import annotations

import ast
import json
import unittest
from pathlib import Path

from magic_glass_box.executor import SandboxPlan
from scripts.check_live_runtime_adapters import (
    ANTHROPIC_MODEL,
    CODEX_MODEL,
    PROOF_TEXT,
    REQUEST_ITEM_ID,
    RESPONSE_ITEM_ID,
    ProviderObservation,
    _anthropic_sse,
    _assert_observations,
    _assert_sandbox_plans,
    _codex_fixture_response,
    _inference_connector_policy,
    _inference_grant_policy,
    _openai_sse,
    _openai_tool_sse,
    _provider_policy,
    _RuntimeProofKrabClient,
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
    def test_runtime_proof_krab_fixture_exposes_ordered_request_and_response(
        self,
    ) -> None:
        client = _RuntimeProofKrabClient()

        request = client.get_session_item(
            "runtime-proof", client.request_item.session_id, REQUEST_ITEM_ID
        )
        response = client.get_session_item(
            "runtime-proof", client.response_item.session_id, RESPONSE_ITEM_ID
        )

        self.assertEqual(request.item_id, REQUEST_ITEM_ID)
        self.assertEqual(response.item_id, RESPONSE_ITEM_ID)
        self.assertEqual(request.history[0].kind, "item.created")
        self.assertEqual(response.history[0].kind, "item.created")
        self.assertLess(request.history[0].event_seq, response.history[0].event_seq)

    def test_live_proofs_do_not_shortcut_the_operator_answer_surface(self) -> None:
        repository = Path(__file__).resolve().parents[1]
        for relative in (
            "scripts/check_live_krab_transport.py",
            "scripts/check_live_runtime_adapters.py",
        ):
            with self.subTest(script=relative):
                tree = ast.parse((repository / relative).read_text(encoding="utf-8"))
                direct_calls = [
                    node
                    for node in ast.walk(tree)
                    if isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "accept_interaction_response"
                ]
                self.assertEqual(direct_calls, [])

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

    def test_openai_tool_fixture_streams_one_bounded_exec_call(self) -> None:
        events = _events(_openai_tool_sse(CODEX_MODEL, "true", 2))

        self.assertEqual(
            [name for name, _ in events],
            [
                "response.created",
                "response.output_item.added",
                "response.function_call_arguments.delta",
                "response.function_call_arguments.done",
                "response.output_item.done",
                "response.completed",
                None,
            ],
        )
        completed = events[-2][1]
        self.assertIsInstance(completed, dict)
        call = completed["response"]["output"][0]  # type: ignore[index]
        self.assertEqual(call["name"], "exec_command")
        self.assertEqual(
            json.loads(call["arguments"]),
            {"cmd": "true", "yield_time_ms": 30_000},
        )

    def test_codex_fixture_drives_handoff_and_replacement_gateway_calls(self) -> None:
        interaction_id = "int_" + "7" * 32

        first = _events(_codex_fixture_response(1, CODEX_MODEL, {}))[-2][1]
        handoff = _events(
            _codex_fixture_response(
                2,
                CODEX_MODEL,
                {"input": [{"output": interaction_id}]},
            )
        )[-2][1]
        describe = _events(_codex_fixture_response(4, CODEX_MODEL, {}))[-2][1]
        read = _events(
            _codex_fixture_response(
                5,
                CODEX_MODEL,
                {"input": [{"output": interaction_id}]},
            )
        )[-2][1]

        commands = []
        for completed in (first, handoff, describe, read):
            self.assertIsInstance(completed, dict)
            arguments = completed["response"]["output"][0]["arguments"]  # type: ignore[index]
            commands.append(json.loads(arguments)["cmd"])
        self.assertIn("node_modules/runtime-proof", commands[0])
        self.assertIn("interaction.open", commands[0])
        self.assertIn("handoff.submit", commands[1])
        self.assertIn(interaction_id, commands[1])
        self.assertIn("run.describe", commands[2])
        self.assertIn("interaction.read", commands[3])
        self.assertIn(interaction_id, commands[3])

        with self.assertRaisesRegex(RuntimeError, "unexpected provider request"):
            _codex_fixture_response(7, CODEX_MODEL, {})

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

    def test_connector_and_fixture_policies_pin_every_authority_boundary(
        self,
    ) -> None:
        openai = _provider_policy(8765, "codex")
        anthropic = _provider_policy(8765, "claude-code")
        connector = _inference_connector_policy(Path("/tmp/runtime-sidecar.sock"))  # noqa: S108
        openai_grant = _inference_grant_policy("codex")
        anthropic_grant = _inference_grant_policy("claude-code")

        self.assertEqual(connector.transport.kind, "unix")
        self.assertEqual(
            [route.name for route in connector.routes],
            ["openai-proof", "anthropic-proof"],
        )
        self.assertEqual(openai_grant.protocol, "openai-responses")
        self.assertEqual(openai_grant.model, CODEX_MODEL)
        self.assertEqual(anthropic_grant.protocol, "anthropic-messages")
        self.assertEqual(anthropic_grant.model, ANTHROPIC_MODEL)
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
        valid_observations = tuple(
            ProviderObservation(
                "openai-responses", True, CODEX_MODEL, True, request_number
            )
            for request_number in range(1, 7)
        ) + (ProviderObservation("anthropic-messages", True, ANTHROPIC_MODEL, True, 1),)
        _assert_observations(valid_observations)
        with self.assertRaisesRegex(RuntimeError, "credential"):
            _assert_observations(
                (
                    ProviderObservation(
                        "openai-responses", False, CODEX_MODEL, True, 1
                    ),
                    *valid_observations[1:],
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
            SandboxPlan(
                "run_" + "3" * 32,
                "magic-glass-box-" + "3" * 32,
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
            _assert_sandbox_plans((escaped, plans[1], plans[2]), ())


if __name__ == "__main__":
    unittest.main()
