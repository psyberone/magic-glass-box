from __future__ import annotations

import json
import unittest
from pathlib import Path

from magic_glass_box.domain import (
    AgentProfileSpec,
    GatewayCapability,
    InteractionKind,
    InteractionState,
    NetworkMode,
    ProjectContext,
    RunState,
    ValidationError,
    WorkspaceMode,
    canonical_json,
    content_sha256,
    require_string,
    validate_handle,
    validate_krab_id,
    validate_name,
    validate_project_key,
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
        self.assertEqual(profile.credential_names, ("krab",))
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


class RunStateTests(unittest.TestCase):
    def test_waiting_round_trip_is_valid(self) -> None:
        validate_transition(RunState.RUNNING, RunState.WAITING_FOR_INPUT)
        validate_transition(RunState.WAITING_FOR_INPUT, RunState.RUNNING)
        validate_transition(RunState.WAITING_FOR_INPUT, RunState.QUEUED)

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


if __name__ == "__main__":
    unittest.main()
