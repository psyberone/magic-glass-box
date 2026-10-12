from __future__ import annotations

import json
import unittest
from pathlib import Path

from magic_glass_box.domain import (
    AgentProfileSpec,
    GatewayCapability,
    HandoffEnvelope,
    InteractionKind,
    InteractionState,
    NetworkMode,
    ProjectContext,
    RunState,
    SupervisionPolicy,
    ValidationError,
    WorkspaceMode,
    canonical_json,
    content_sha256,
    require_string,
    validate_handle,
    validate_krab_id,
    validate_name,
    validate_project_key,
    validate_provider_name,
    validate_transition,
)


class ScalarValidationTests(unittest.TestCase):
    def test_require_string_enforces_type_empty_nul_and_utf8_byte_limit(self) -> None:
        self.assertEqual(
            require_string("é", field="value", max_bytes=2),
            "é",
        )
        self.assertEqual(
            require_string("", field="value", max_bytes=0, allow_empty=True),
            "",
        )
        for value, message in (
            (7, "must be text"),
            ("", "must not be empty"),
            ("a\x00b", "must not contain NUL"),
            ("é", "exceeds 1 UTF-8 bytes"),
        ):
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(ValidationError, message),
            ):
                require_string(value, field="value", max_bytes=1)

    def test_identifier_boundaries_and_near_misses(self) -> None:
        self.assertEqual(validate_handle("abc"), "abc")
        self.assertEqual(validate_handle("a" + "1" * 31), "a" + "1" * 31)
        self.assertEqual(validate_name("A"), "A")
        self.assertEqual(validate_name("A" + " " * 63), "A" + " " * 63)
        self.assertEqual(validate_project_key("abc"), "abc")
        self.assertEqual(validate_project_key("a" + "1" * 31), "a" + "1" * 31)
        user_id = "usr_" + "a" * 32
        self.assertEqual(validate_krab_id(user_id, "usr"), user_id)

        invalid = (
            (validate_handle, ("ab",)),
            (validate_handle, ("Aaa",)),
            (validate_handle, ("a" + "1" * 32,)),
            (validate_name, ("",)),
            (validate_name, ("A/",)),
            (validate_name, ("A" + " " * 64,)),
            (validate_project_key, ("ab",)),
            (validate_project_key, ("a_b",)),
            (validate_project_key, ("a" + "1" * 32,)),
            (validate_krab_id, ("prj_" + "a" * 32, "usr")),
            (validate_krab_id, ("usr_" + "g" * 32, "usr")),
        )
        for validator, arguments in invalid:
            with (
                self.subTest(validator=validator.__name__, arguments=arguments),
                self.assertRaises(ValidationError),
            ):
                validator(*arguments)

        with self.assertRaises(ValidationError) as bad_handle:
            validate_handle("Bad")
        self.assertEqual(
            str(bad_handle.exception),
            "handle must match [a-z][a-z0-9_-]{2,31}",
        )
        with self.assertRaises(ValidationError) as bad_name:
            validate_name("/")
        self.assertEqual(
            str(bad_name.exception),
            "name must be 1 to 64 restricted display characters",
        )

    def test_canonical_json_is_finite_sorted_and_compact(self) -> None:
        canonical = canonical_json({"z": "å", "a": [1, True, None]})
        self.assertEqual(canonical, '{"a":[1,true,null],"z":"å"}')
        self.assertEqual(
            content_sha256(canonical),
            "bf339568b80da36b004b92e1471a9a0d49f79a2110af8918f30be623358749c8",
        )
        for value in (float("nan"), float("inf"), {"bad": object()}):
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(ValidationError, "finite JSON data"),
            ):
                canonical_json(value)

        with self.assertRaises(ValidationError) as non_finite:
            canonical_json(float("nan"))
        self.assertEqual(
            str(non_finite.exception),
            "value must be finite JSON data",
        )

    def test_provider_name_default_field_is_part_of_the_contract(self) -> None:
        with self.assertRaises(ValidationError) as invalid:
            validate_provider_name("NOT VALID")
        self.assertEqual(
            str(invalid.exception),
            "provider name must match [a-z][a-z0-9_-]{0,63}",
        )


class ProjectContextTests(unittest.TestCase):
    def test_context_is_canonical_and_content_addressed(self) -> None:
        first = ProjectContext.from_mapping(
            {
                "purpose": "Maintain the service",
                "instructions": ["Run tests"],
                "test_commands": [["python", "-m", "unittest"]],
                "allowed_tools": ["python", "git"],
            }
        )
        second = ProjectContext.from_mapping(
            {
                "allowed_tools": ["python", "git"],
                "test_commands": [["python", "-m", "unittest"]],
                "instructions": ["Run tests"],
                "purpose": "Maintain the service",
            }
        )
        self.assertEqual(first.canonical, second.canonical)
        self.assertEqual(first.digest, second.digest)
        self.assertEqual(len(first.digest), 64)
        self.assertEqual(
            first.to_dict(),
            {
                "allowed_tools": ["python", "git"],
                "build_commands": [],
                "default_branch": "main",
                "instructions": ["Run tests"],
                "purpose": "Maintain the service",
                "repository_path": None,
                "test_commands": [["python", "-m", "unittest"]],
            },
        )

    def test_context_rejects_shell_strings_and_unknown_fields(self) -> None:
        with self.assertRaisesRegex(ValidationError, "argv array"):
            ProjectContext.from_mapping(
                {"purpose": "Build", "build_commands": ["make && upload"]}
            )
        with self.assertRaisesRegex(ValidationError, "unknown fields"):
            ProjectContext.from_mapping({"purpose": "Build", "secrets": ["all"]})

    def test_context_collection_limits_and_types(self) -> None:
        valid = ProjectContext.from_mapping(
            {
                "purpose": "Build",
                "instructions": [str(index) for index in range(128)],
                "build_commands": [["x"] * 64] * 32,
                "allowed_tools": [f"tool-{index}" for index in range(128)],
                "repository_path": "/workspace/project",
                "default_branch": "feature/test",
            }
        )
        self.assertEqual(len(valid.instructions), 128)
        self.assertEqual(len(valid.build_commands), 32)
        self.assertEqual(valid.repository_path, "/workspace/project")
        self.assertEqual(valid.default_branch, "feature/test")

        invalid_contexts = (
            ({"purpose": "Build", "instructions": "not-a-list"}, "at most 128"),
            (
                {"purpose": "Build", "instructions": ["same", "same"]},
                "unique",
            ),
            (
                {"purpose": "Build", "instructions": ["x"] * 129},
                "at most 128",
            ),
            (
                {"purpose": "Build", "build_commands": [["x"]] * 33},
                "at most 32",
            ),
            ({"purpose": "Build", "build_commands": [[]]}, "non-empty"),
            (
                {"purpose": "Build", "build_commands": [["x"] * 65]},
                "non-empty",
            ),
            (
                {"purpose": "Build", "build_commands": [["x" * 1025]]},
                "1024 UTF-8 bytes",
            ),
        )
        for value, message in invalid_contexts:
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(ValidationError, message),
            ):
                ProjectContext.from_mapping(value)

        with self.assertRaises(ValidationError) as bad_instruction:
            ProjectContext.from_mapping(
                {"purpose": "Build", "instructions": ["bad\x00instruction"]}
            )
        self.assertEqual(
            str(bad_instruction.exception),
            "instructions entry must not contain NUL",
        )
        with self.assertRaises(ValidationError) as bad_argument:
            ProjectContext.from_mapping(
                {"purpose": "Build", "test_commands": [["bad\x00argument"]]}
            )
        self.assertEqual(
            str(bad_argument.exception),
            "test_commands[0] argument must not contain NUL",
        )


class AgentProfileTests(unittest.TestCase):
    def test_published_profile_uses_the_run_scoped_gateway(self) -> None:
        path = Path(__file__).parents[1] / "examples" / "agent-profile.json"
        profile = AgentProfileSpec.from_mapping(
            json.loads(path.read_text(encoding="utf-8"))
        )
        self.assertEqual(profile.network_mode, NetworkMode.GATEWAY_ONLY)
        self.assertEqual(profile.credential_names, ("krab", "openai-main"))
        self.assertEqual(profile.gateway_capabilities, tuple(GatewayCapability))

    def test_profile_uses_argv_and_named_credentials(self) -> None:
        profile = AgentProfileSpec.from_mapping(
            {
                "image": "agent@sha256:abc",
                "command": ["codex", "exec"],
                "credential_names": ["gh", "krab"],
            }
        )
        self.assertEqual(profile.command, ("codex", "exec"))
        self.assertEqual(profile.credential_names, ("gh", "krab"))
        self.assertEqual(profile.workspace_mode, WorkspaceMode.COPY_ON_WRITE)
        self.assertEqual(profile.network_mode, NetworkMode.GATEWAY_ONLY)
        self.assertEqual(profile.gateway_capabilities, ())
        self.assertEqual(profile.runtime_adapter, "command")
        self.assertEqual(
            profile.to_dict(),
            {
                "command": ["codex", "exec"],
                "credential_names": ["gh", "krab"],
                "image": "agent@sha256:abc",
                "network_mode": "gateway_only",
                "workspace_mode": "copy_on_write",
            },
        )

    def test_profile_pins_a_runtime_adapter_without_changing_legacy_digests(
        self,
    ) -> None:
        legacy = AgentProfileSpec.from_mapping(
            {"image": "agent@sha256:abc", "command": ["agent", "run"]}
        )
        codex = AgentProfileSpec.from_mapping(
            {
                "image": "agent@sha256:abc",
                "command": ["codex"],
                "runtime_adapter": "codex",
                "runtime_provider_grant": "openai-main",
                "credential_names": ["openai-main"],
                "gateway_capabilities": ["run.describe", "provider.invoke"],
            }
        )

        self.assertNotIn("runtime_adapter", legacy.to_dict())
        self.assertEqual(codex.runtime_adapter, "codex")
        self.assertEqual(codex.to_dict()["runtime_adapter"], "codex")
        self.assertEqual(codex.runtime_provider_grant, "openai-main")
        with self.assertRaisesRegex(ValidationError, "runtime_adapter must match"):
            AgentProfileSpec.from_mapping(
                {
                    "image": "agent@sha256:abc",
                    "command": ["agent"],
                    "runtime_adapter": "Bad Adapter",
                }
            )

    def test_provider_gateway_capability_requires_a_named_provider_grant(self) -> None:
        profile = AgentProfileSpec.from_mapping(
            {
                "image": "agent@sha256:abc",
                "command": ["agent"],
                "credential_names": ["krab", "openai-main"],
                "gateway_capabilities": ["provider.invoke"],
            }
        )
        self.assertEqual(
            profile.gateway_capabilities,
            (GatewayCapability.PROVIDER_INVOKE,),
        )
        with self.assertRaisesRegex(ValidationError, "provider grant"):
            AgentProfileSpec.from_mapping(
                {
                    "image": "agent",
                    "command": ["run"],
                    "credential_names": ["krab"],
                    "gateway_capabilities": ["provider.invoke"],
                }
            )

    def test_vendor_runtime_requires_one_explicit_provider_transport(self) -> None:
        valid = {
            "image": "agent@sha256:abc",
            "command": ["codex"],
            "runtime_adapter": "codex",
            "runtime_provider_grant": "openai-main",
            "credential_names": ["krab", "openai-main"],
            "gateway_capabilities": ["run.describe", "provider.invoke"],
        }
        profile = AgentProfileSpec.from_mapping(valid)
        self.assertEqual(profile.runtime_provider_grant, "openai-main")
        self.assertEqual(profile.to_dict()["runtime_provider_grant"], "openai-main")

        cases = (
            ({"runtime_provider_grant": "other"}, "credential_names"),
            ({"gateway_capabilities": ["provider.invoke"]}, "run.describe"),
            ({"gateway_capabilities": ["run.describe"]}, "provider.invoke"),
        )
        for override, message in cases:
            with (
                self.subTest(override=override),
                self.assertRaisesRegex(ValidationError, message),
            ):
                AgentProfileSpec.from_mapping({**valid, **override})

        legacy_vendor = AgentProfileSpec.from_mapping(
            {
                "image": "agent@sha256:abc",
                "command": ["codex"],
                "runtime_adapter": "codex",
            }
        )
        self.assertIsNone(legacy_vendor.runtime_provider_grant)
        self.assertNotIn("runtime_provider_grant", legacy_vendor.to_dict())

    def test_gateway_capabilities_are_explicit_and_require_gateway_authority(
        self,
    ) -> None:
        profile = AgentProfileSpec.from_mapping(
            {
                "image": "agent@sha256:abc",
                "command": ["agent"],
                "credential_names": ["krab"],
                "gateway_capabilities": [
                    "run.describe",
                    "interaction.open",
                    "interaction.read",
                ],
            }
        )
        self.assertEqual(
            profile.gateway_capabilities,
            (
                GatewayCapability.RUN_DESCRIBE,
                GatewayCapability.INTERACTION_OPEN,
                GatewayCapability.INTERACTION_READ,
            ),
        )

        for value, message in (
            (
                {
                    "image": "agent",
                    "command": ["run"],
                    "network_mode": "none",
                    "gateway_capabilities": ["run.describe"],
                },
                "require gateway_only",
            ),
            (
                {
                    "image": "agent",
                    "command": ["run"],
                    "gateway_capabilities": ["interaction.open"],
                },
                "require the krab credential",
            ),
            (
                {
                    "image": "agent",
                    "command": ["run"],
                    "gateway_capabilities": ["provider.anywhere"],
                },
                "gateway capability is invalid",
            ),
            (
                {
                    "image": "agent",
                    "command": ["run"],
                    "gateway_capabilities": ["run.describe", "run.describe"],
                },
                "unique",
            ),
        ):
            with (
                self.subTest(message=message),
                self.assertRaisesRegex(ValidationError, message),
            ):
                AgentProfileSpec.from_mapping(value)

    def test_profile_rejects_unknown_policy(self) -> None:
        with self.assertRaises(ValidationError):
            AgentProfileSpec.from_mapping(
                {
                    "image": "agent:latest",
                    "command": ["agent"],
                    "network_mode": "internet",
                }
            )

    def test_profile_rejects_invalid_shape_and_credential_sets(self) -> None:
        for value, message in (
            ({"image": "agent"}, "requires image and command"),
            ({"image": "agent", "command": []}, "non-empty argv"),
            (
                {"image": "agent", "command": ["run"], "extra": True},
                "unknown fields",
            ),
            (
                {
                    "image": "agent",
                    "command": ["run"],
                    "credential_names": ["Bad"],
                },
                "credential names must match",
            ),
            (
                {
                    "image": "agent",
                    "command": ["run"],
                    "credential_names": ["same", "same"],
                },
                "unique handles",
            ),
            (
                {
                    "image": "agent",
                    "command": ["run"],
                    "credential_names": [f"c{index}" for index in range(33)],
                },
                "at most 32",
            ),
        ):
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(ValidationError, message),
            ):
                AgentProfileSpec.from_mapping(value)


class HandoffEnvelopeTests(unittest.TestCase):
    def valid(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "work_state": "waiting_for_input",
            "summary": "Implementation is paused at a bounded decision.",
            "decisions": ["Keep the source checkout immutable."],
            "artifacts": [
                {
                    "path": "src/magic_glass_box/domain.py",
                    "description": "Domain contract inspected by the worker.",
                }
            ],
            "next_steps": ["Read the accepted response and continue."],
            "pending_interaction_ids": ["int_" + "1" * 32],
        }

    def test_round_trip_is_canonical_bounded_work_state(self) -> None:
        value = self.valid()

        handoff = HandoffEnvelope.from_mapping(value)

        self.assertEqual(handoff.to_dict(), value)
        self.assertEqual(json.loads(handoff.canonical), value)
        self.assertEqual(handoff.digest, content_sha256(handoff.canonical))

    def test_published_schema_and_example_match_the_v1_contract(self) -> None:
        root = Path(__file__).parents[1]
        schema = json.loads(
            (root / "schemas" / "handoff-v1.schema.json").read_text(encoding="utf-8")
        )
        example = json.loads(
            (root / "examples" / "handoff.json").read_text(encoding="utf-8")
        )

        self.assertFalse(schema["additionalProperties"])
        self.assertEqual(schema["properties"]["schema_version"]["const"], 1)
        self.assertEqual(set(schema["required"]), set(example))
        self.assertEqual(HandoffEnvelope.from_mapping(example).to_dict(), example)

    def test_rejects_transcript_fields_and_unbounded_or_unsafe_content(self) -> None:
        valid = self.valid()
        cases = (
            ({**valid, "transcript": ["secret"]}, "unknown fields"),
            ({key: item for key, item in valid.items() if key != "summary"}, "missing"),
            ({**valid, "schema_version": True}, "schema_version"),
            ({**valid, "work_state": "chat_log"}, "work_state"),
            ({**valid, "summary": "   "}, "blank"),
            ({**valid, "decisions": ["same", "same"]}, "unique"),
            ({**valid, "next_steps": [""]}, "empty"),
            (
                {
                    **valid,
                    "artifacts": [{"path": "../outside", "description": "unsafe"}],
                },
                "safe relative POSIX path",
            ),
            (
                {
                    **valid,
                    "artifacts": [
                        {"path": "same.py", "description": "one"},
                        {"path": "same.py", "description": "two"},
                    ],
                },
                "unique paths",
            ),
            (
                {**valid, "pending_interaction_ids": ["int_bad"]},
                "interaction identifier",
            ),
            (
                {
                    **valid,
                    "pending_interaction_ids": [
                        "int_" + "1" * 32,
                        "int_" + "1" * 32,
                    ],
                },
                "unique",
            ),
        )
        for candidate, message in cases:
            with (
                self.subTest(message=message),
                self.assertRaisesRegex(ValidationError, message),
            ):
                HandoffEnvelope.from_mapping(candidate)


class RunStateTests(unittest.TestCase):
    def test_waiting_requires_a_replacement_attempt(self) -> None:
        validate_transition(RunState.RUNNING, RunState.WAITING_FOR_INPUT)
        validate_transition(RunState.WAITING_FOR_INPUT, RunState.QUEUED)
        with self.assertRaisesRegex(ValidationError, "invalid run transition"):
            validate_transition(RunState.WAITING_FOR_INPUT, RunState.RUNNING)

    def test_interaction_values_are_stable(self) -> None:
        self.assertEqual(
            [kind.value for kind in InteractionKind],
            ["question", "approval", "choice"],
        )
        self.assertEqual(
            [state.value for state in InteractionState],
            ["open", "answered", "cancelled"],
        )

    def test_terminal_state_cannot_transition(self) -> None:
        with self.assertRaisesRegex(ValidationError, "invalid run transition"):
            validate_transition(RunState.SUCCEEDED, RunState.RUNNING)


class SupervisionPolicyTests(unittest.TestCase):
    def test_policy_is_explicit_bounded_and_canonical(self) -> None:
        policy = SupervisionPolicy.from_mapping(
            {
                "schema_version": 1,
                "auto_execute": True,
                "retry_backoff_seconds": [5, 30, 300],
                "interaction_escalation_seconds": [1800, 7200],
                "attempt_idle_escalation_seconds": [3600],
                "attempt_lease_expired_escalation_seconds": [0, 900],
                "other_attention_escalation_seconds": [1800],
            }
        )

        self.assertTrue(policy.auto_execute)
        self.assertEqual(policy.retry_backoff_seconds, (5, 30, 300))
        self.assertEqual(policy.attempt_lease_expired_escalation_seconds, (0, 900))
        self.assertEqual(policy.to_dict()["schema_version"], 1)
        self.assertEqual(
            SupervisionPolicy.from_mapping(policy.to_dict()).digest,
            policy.digest,
        )

    def test_disabled_policy_is_safe_and_invalid_bounds_fail_closed(self) -> None:
        self.assertEqual(
            SupervisionPolicy.disabled().to_dict(),
            {
                "schema_version": 1,
                "auto_execute": False,
                "retry_backoff_seconds": [5, 30, 300],
                "interaction_escalation_seconds": [],
                "attempt_idle_escalation_seconds": [],
                "attempt_lease_expired_escalation_seconds": [],
                "other_attention_escalation_seconds": [],
            },
        )
        valid = SupervisionPolicy.disabled().to_dict()
        cases = (
            ({**valid, "unexpected": True}, "unknown fields"),
            ({**valid, "schema_version": True}, "schema_version"),
            ({**valid, "auto_execute": 1}, "auto_execute"),
            ({**valid, "retry_backoff_seconds": [30, 5]}, "strictly increasing"),
            (
                {**valid, "interaction_escalation_seconds": [60, 60]},
                "strictly increasing",
            ),
            (
                {**valid, "attempt_idle_escalation_seconds": [-1]},
                "between 0 and",
            ),
            (
                {**valid, "other_attention_escalation_seconds": [1] * 9},
                "at most 8",
            ),
        )
        for candidate, message in cases:
            with (
                self.subTest(message=message),
                self.assertRaisesRegex(ValidationError, message),
            ):
                SupervisionPolicy.from_mapping(candidate)


if __name__ == "__main__":
    unittest.main()
