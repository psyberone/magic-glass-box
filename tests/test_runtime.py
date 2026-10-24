from __future__ import annotations

import unittest

from magic_glass_box.runtime import (
    MAX_RUNTIME_ARGUMENT_BYTES,
    MAX_RUNTIME_ARGUMENTS,
    MAX_RUNTIME_ENVIRONMENT,
    MAX_RUNTIME_ENVIRONMENT_BYTES,
    MAX_RUNTIME_OUTPUT_BYTES,
    ClaudeCodeRuntimeAdapter,
    CodexRuntimeAdapter,
    CommandRuntimeAdapter,
    RuntimeAdapterError,
    RuntimeAdapterRegistry,
    RuntimeCompletion,
    RuntimeLaunch,
    default_runtime_adapters,
)


class RuntimeAdapterContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.registry = RuntimeAdapterRegistry(default_runtime_adapters())

    def test_built_in_adapters_have_stable_distinct_launch_contracts(self) -> None:
        command = self.registry.launch("command", ("worker", "--once"))
        codex = self.registry.launch("codex", ("codex",), provider_grant="openai-main")
        claude = self.registry.launch(
            "claude-code", ("claude",), provider_grant="anthropic-main"
        )

        self.assertEqual(command, RuntimeLaunch(("worker", "--once")))
        self.assertEqual(
            codex.command[:5],
            (
                "magic-glass-box-worker",
                "serve",
                "--dialect",
                "openai-responses",
                "--grant",
            ),
        )
        self.assertEqual(codex.command[5:8], ("openai-main", "--", "codex"))
        self.assertIn(
            'model_providers.magic_glass_box.base_url="http://127.0.0.1:8766/v1"',
            codex.command,
        )
        for argument in (
            "exec",
            "--json",
            "--ephemeral",
            "--ignore-user-config",
            "--skip-git-repo-check",
            "--dangerously-bypass-approvals-and-sandbox",
        ):
            self.assertIn(argument, codex.command)
        self.assertIn('web_search="disabled"', codex.command)
        self.assertEqual(
            claude.command[:8],
            (
                "magic-glass-box-worker",
                "serve",
                "--dialect",
                "anthropic-messages",
                "--grant",
                "anthropic-main",
                "--",
                "claude",
            ),
        )
        self.assertEqual(
            claude.command[8:-1],
            (
                "--print",
                "--output-format",
                "json",
                "--no-session-persistence",
                "--permission-prompts",
                "none",
                "--dangerously-skip-permissions",
                "--bare",
            ),
        )
        self.assertEqual(codex.command[-1], claude.command[-1])
        self.assertIn("run.describe", codex.command[-1])
        self.assertIn("MAGIC_GLASS_BOX_GATEWAY_DIR", codex.command[-1])
        self.assertIn("magic-glass-box-worker call", codex.command[-1])
        self.assertIn("review_assignment", codex.command[-1])
        self.assertIn("`review_outcome`", codex.command[-1])
        self.assertIn('"schema_version": 2', codex.command[-1])
        self.assertNotIn("summary begins", codex.command[-1])
        self.assertIn("changes_requested", codex.command[-1])
        self.assertIn("not modify the workspace", codex.command[-1])
        self.assertIn("smallest\nproject-native primitive", codex.command[-1])
        self.assertIn("implement the requested corrections", codex.command[-1])
        self.assertIn("maintainer_review", codex.command[-1])
        self.assertIn("has not been rejected", codex.command[-1])
        self.assertNotIn("openai-main", codex.environment)
        self.assertIn(
            ("MAGIC_GLASS_BOX_PROVIDER_TOKEN", "local-gateway-not-a-secret"),
            codex.environment,
        )
        self.assertIn(
            ("ANTHROPIC_BASE_URL", "http://127.0.0.1:8766"),
            claude.environment,
        )
        self.assertIn(
            ("ANTHROPIC_API_KEY", "local-gateway-not-a-secret"),
            claude.environment,
        )

    def test_built_in_adapters_normalize_their_different_success_outputs(self) -> None:
        command = self.registry.complete("command", exit_code=0, stdout="anything")
        codex = self.registry.complete(
            "codex",
            exit_code=0,
            stdout=(
                '{"type":"thread.started","thread_id":"abc"}\n'
                '{"type":"turn.completed","usage":{"input_tokens":1}}\n'
            ),
        )
        claude = self.registry.complete(
            "claude-code",
            exit_code=0,
            stdout=(
                '{"type":"result","subtype":"success",'
                '"is_error":false,"result":"done"}\n'
            ),
        )

        self.assertEqual(command, RuntimeCompletion(exit_code=0))
        self.assertEqual(codex, RuntimeCompletion(exit_code=0))
        self.assertEqual(claude, RuntimeCompletion(exit_code=0))

    def test_codex_jsonl_preserves_unicode_line_and_paragraph_separators(self) -> None:
        stdout = (
            '{"type":"item.completed","text":"before\u2028middle\u2029after"}\n'
            '{"type":"turn.completed"}\n'
        )

        self.assertEqual(
            self.registry.complete("codex", exit_code=0, stdout=stdout),
            RuntimeCompletion(exit_code=0),
        )

    def test_nonzero_exit_is_preserved_without_parsing_private_output(self) -> None:
        for name in self.registry.names:
            with self.subTest(name=name):
                self.assertEqual(
                    self.registry.complete(
                        name,
                        exit_code=17,
                        stdout="not valid structured output and SECRET",
                    ),
                    RuntimeCompletion(exit_code=17),
                )

    def test_structured_adapters_fail_closed_on_malformed_success_output(self) -> None:
        cases = (
            ("codex", ""),
            ("codex", "not-json\n"),
            ("codex", '{"type":"turn.started"}\n'),
            ("codex", '{"type":"turn.completed","type":"other"}\n'),
            ("claude-code", "[]"),
            ("claude-code", '{"type":"result","subtype":"error"}'),
            (
                "claude-code",
                '{"type":"result","subtype":"success","is_error":true}',
            ),
            (
                "claude-code",
                '{"type":"result","subtype":"success",'
                '"is_error":false,"is_error":false}',
            ),
        )
        for name, stdout in cases:
            with (
                self.subTest(name=name, stdout=stdout),
                self.assertRaises(RuntimeAdapterError) as caught,
            ):
                self.registry.complete(name, exit_code=0, stdout=stdout)
            if stdout:
                self.assertNotIn(stdout, str(caught.exception))

    def test_runtime_launch_and_completion_values_are_bounded(self) -> None:
        with self.assertRaisesRegex(RuntimeAdapterError, "non-empty argv"):
            RuntimeLaunch(())
        with self.assertRaisesRegex(RuntimeAdapterError, "environment name"):
            RuntimeLaunch(("worker",), (("bad-name", "value"),))
        with self.assertRaisesRegex(RuntimeAdapterError, "unique"):
            RuntimeLaunch(("worker",), (("MODE", "one"), ("MODE", "two")))
        with self.assertRaisesRegex(RuntimeAdapterError, "exit code"):
            RuntimeCompletion(exit_code=True)  # type: ignore[arg-type]
        with self.assertRaisesRegex(RuntimeAdapterError, "output exceeds"):
            self.registry.complete(
                "codex",
                exit_code=0,
                stdout="x" * (MAX_RUNTIME_OUTPUT_BYTES + 1),
            )

    def test_vendor_completion_accepts_output_beyond_the_old_one_mib_cap(self) -> None:
        verbose_event = (
            '{"type":"item.completed","text":"' + ("x" * (2 * 1024 * 1024)) + '"}\n'
        )
        stdout = verbose_event + '{"type":"turn.completed"}\n'

        self.assertEqual(
            self.registry.complete("codex", exit_code=0, stdout=stdout),
            RuntimeCompletion(0),
        )

    def test_runtime_value_boundaries_reject_malformed_values(self) -> None:
        invalid_commands = (
            ("",),
            ("worker\x00argument",),
            (7,),
            ("x" * (MAX_RUNTIME_ARGUMENT_BYTES + 1),),
            tuple("worker" for _ in range(MAX_RUNTIME_ARGUMENTS + 1)),
        )
        for command in invalid_commands:
            with (
                self.subTest(command=command[:2]),
                self.assertRaises(RuntimeAdapterError),
            ):
                RuntimeLaunch(command)  # type: ignore[arg-type]

        invalid_environments = (
            [("MODE", "value")],
            tuple(
                (f"MODE_{index}", "value")
                for index in range(MAX_RUNTIME_ENVIRONMENT + 1)
            ),
            (("MODE",),),
            ((7, "value"),),
            (("MODE", 7),),
            (("MODE", "value\x00suffix"),),
            (("MODE", "x" * (MAX_RUNTIME_ENVIRONMENT_BYTES + 1)),),
        )
        for environment in invalid_environments:
            with (
                self.subTest(environment=environment[:2]),
                self.assertRaises(RuntimeAdapterError),
            ):
                RuntimeLaunch(("worker",), environment)  # type: ignore[arg-type]

        self.assertEqual(
            RuntimeLaunch(
                ("x" * MAX_RUNTIME_ARGUMENT_BYTES,),
                (("MODE", "x" * MAX_RUNTIME_ENVIRONMENT_BYTES),),
            ).environment[0][0],
            "MODE",
        )
        self.assertEqual(
            self.registry.complete(
                "command",
                exit_code=0,
                stdout="x" * MAX_RUNTIME_OUTPUT_BYTES,
            ),
            RuntimeCompletion(0),
        )

        for exit_code in (-1, 256, 1.5):
            with (
                self.subTest(exit_code=exit_code),
                self.assertRaisesRegex(RuntimeAdapterError, "exit code"),
            ):
                RuntimeCompletion(exit_code)  # type: ignore[arg-type]

    def test_registry_rejects_unknown_duplicate_and_nonconforming_adapters(
        self,
    ) -> None:
        with self.assertRaisesRegex(RuntimeAdapterError, "unknown runtime adapter"):
            self.registry.launch("missing", ("worker",))
        with self.assertRaisesRegex(RuntimeAdapterError, "duplicate"):
            RuntimeAdapterRegistry((CommandRuntimeAdapter(), CommandRuntimeAdapter()))

        class InvalidLaunchAdapter:
            name = "invalid"

            def launch(
                self,
                _command: tuple[str, ...],
                *,
                provider_grant: str | None,
            ) -> object:
                return ("not", "a", "launch")

            def complete(self, *, exit_code: int, stdout: str) -> object:
                return exit_code, stdout

        invalid = RuntimeAdapterRegistry((InvalidLaunchAdapter(),))  # type: ignore[arg-type]
        with self.assertRaisesRegex(RuntimeAdapterError, "invalid launch"):
            invalid.launch("invalid", ("worker",))
        with self.assertRaisesRegex(RuntimeAdapterError, "invalid completion"):
            invalid.complete("invalid", exit_code=0, stdout="")

    def test_registry_normalizes_sequences_but_rejects_text_as_argv(self) -> None:
        self.assertEqual(
            self.registry.launch("command", ["worker", "--once"]),
            RuntimeLaunch(("worker", "--once")),
        )
        self.assertEqual(self.registry.require("codex"), "codex")
        with self.assertRaisesRegex(RuntimeAdapterError, "argv"):
            self.registry.launch("command", "worker")
        with self.assertRaisesRegex(RuntimeAdapterError, "at least one"):
            RuntimeAdapterRegistry(())

        class NamelessAdapter:
            pass

        with self.assertRaisesRegex(RuntimeAdapterError, "runtime adapter"):
            RuntimeAdapterRegistry((NamelessAdapter(),))  # type: ignore[arg-type]

    def test_structured_runtime_envelopes_reject_wrong_shapes_and_constants(
        self,
    ) -> None:
        cases = (
            ("codex", "[]\n", "invalid JSONL"),
            ("codex", '{"type":7}\n', "invalid JSONL"),
            ("codex", "\n  \n", "incomplete JSONL"),
            ("claude-code", "NaN", "claude-code produced invalid JSON"),
        )
        for name, stdout, message in cases:
            with (
                self.subTest(name=name, stdout=stdout),
                self.assertRaisesRegex(RuntimeAdapterError, message),
            ):
                self.registry.complete(name, exit_code=0, stdout=stdout)

    def test_vendor_adapters_require_an_executable_not_profile_owned_flags(
        self,
    ) -> None:
        for adapter in (CodexRuntimeAdapter(), ClaudeCodeRuntimeAdapter()):
            with (
                self.subTest(name=adapter.name),
                self.assertRaisesRegex(RuntimeAdapterError, "executable only"),
            ):
                adapter.launch(
                    (adapter.name, "--profile-owned-flag"),
                    provider_grant="provider-main",
                )

    def test_vendor_adapters_require_an_explicit_provider_grant(self) -> None:
        for name in ("codex", "claude-code"):
            with (
                self.subTest(name=name),
                self.assertRaisesRegex(RuntimeAdapterError, "provider grant"),
            ):
                self.registry.launch(name, (name,))

        with self.assertRaisesRegex(RuntimeAdapterError, "provider grant"):
            self.registry.launch("codex", ("codex",), provider_grant="Bad Grant")


if __name__ == "__main__":
    unittest.main()
