from __future__ import annotations

import ast
import importlib
import inspect
import unittest


class LiveProofStorageAdmissionTests(unittest.TestCase):
    def test_every_docker_proof_requires_an_explicit_unenforced_storage_choice(
        self,
    ) -> None:
        cases = (
            ("scripts.check_live_gateway", []),
            ("scripts.check_live_provider_gateway", []),
            ("scripts.check_live_runtime_adapters", []),
            (
                "scripts.check_live_imitation_krab_beta",
                [
                    "--profile-dir",
                    "/private/profile",
                    "--repository",
                    "/private/repository",
                    "--state-dir",
                    "/private/state",
                    "--model",
                    "test-model",
                ],
            ),
            (
                "scripts.check_live_imitation_krab_maintainer_round",
                [
                    "--profile-dir",
                    "/private/profile",
                    "--repository",
                    "/private/repository",
                    "--state-dir",
                    "/private/state",
                    "--candidate",
                    "candidate",
                    "--review",
                    "/private/review.json",
                    "--model",
                    "test-model",
                ],
            ),
            (
                "scripts.check_live_imitation_krab_workflow",
                [
                    "--profile-dir",
                    "/private/profile",
                    "--repository",
                    "/private/repository",
                    "--state-dir",
                    "/private/state",
                    "--model",
                    "test-model",
                ],
            ),
            (
                "scripts.check_live_m13",
                [
                    "--profile-dir",
                    "/private/profile",
                    "--repository",
                    "/private/repository",
                    "--state-dir",
                    "/private/state",
                    "--model",
                    "test-model",
                ],
            ),
            (
                "scripts.check_live_openai_chatgpt",
                [
                    "--profile-dir",
                    "/private/profile",
                    "--model",
                    "test-model",
                ],
            ),
        )

        for module_name, required in cases:
            module = importlib.import_module(module_name)
            with self.subTest(module=module_name):
                defaults = module.parse_args(required)
                enabled = module.parse_args(
                    [*required, "--allow-unenforced-worker-storage"]
                )
                self.assertFalse(defaults.allow_unenforced_worker_storage)
                self.assertTrue(enabled.allow_unenforced_worker_storage)

                tree = ast.parse(inspect.getsource(module))
                constructors = [
                    node
                    for node in ast.walk(tree)
                    if isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id == "DockerExecutor"
                ]
                self.assertTrue(constructors)
                for constructor in constructors:
                    forwarded = next(
                        (
                            keyword.value
                            for keyword in constructor.keywords
                            if keyword.arg == "allow_unenforced_worker_storage"
                        ),
                        None,
                    )
                    self.assertIsInstance(forwarded, ast.Attribute)
                    self.assertEqual(forwarded.attr, "allow_unenforced_worker_storage")
