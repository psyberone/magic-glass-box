from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tempfile
import tomllib
import types
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
CHECKER = ROOT / "scripts" / "check_mutation_score.py"
MAKEFILE = ROOT / "Makefile"
MUTMUT_RUNNER = ROOT / "scripts" / "run_mutmut.py"
PYPROJECT = ROOT / "pyproject.toml"
PANOPTICON = ROOT / "panopticon.yml"
DEVELOPMENT_LOCK = ROOT / "requirements-development.lock"
RUNTIME_DOCKERFILE = ROOT / "docker" / "runtime-proof.Dockerfile"


class MutationScoreGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.stats = Path(self.temporary.name) / "stats.json"

    @staticmethod
    def complete_stats(*, killed: int, survived: int) -> dict[str, int]:
        return {
            "check_was_interrupted_by_user": 0,
            "killed": killed,
            "no_tests": 0,
            "segfault": 0,
            "skipped": 0,
            "suspicious": 0,
            "survived": survived,
            "timeout": 0,
            "total": killed + survived,
        }

    def run_checker(
        self, payload: object, *, minimum: str = "80"
    ) -> subprocess.CompletedProcess[str]:
        self.stats.write_text(json.dumps(payload), encoding="utf-8")
        return subprocess.run(  # noqa: S603 - fixed interpreter and local script
            [
                sys.executable,
                str(CHECKER),
                "--stats",
                str(self.stats),
                "--minimum",
                minimum,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )

    def test_accepts_score_at_floor(self) -> None:
        result = self.run_checker(self.complete_stats(killed=8, survived=2))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("80.00%", result.stdout)

    def test_rejects_score_below_floor(self) -> None:
        result = self.run_checker(self.complete_stats(killed=7, survived=3))
        self.assertEqual(result.returncode, 1)
        self.assertIn("below required 80.00%", result.stderr)

    def test_rejects_malformed_or_empty_stats(self) -> None:
        for payload in (
            {"killed": True, "total": 10},
            {"killed": 0, "total": 0},
            {"killed": 8, "total": 10},
            {
                **self.complete_stats(killed=8, survived=2),
                "total": 11,
            },
        ):
            with self.subTest(payload=payload):
                result = self.run_checker(payload)
                self.assertEqual(result.returncode, 2)
                self.assertIn("invalid mutation stats", result.stderr)

    def test_uses_exact_arithmetic_at_the_percentage_boundary(self) -> None:
        killed = 18_014_398_509_481_984
        result = self.run_checker(
            self.complete_stats(killed=killed, survived=1),
            minimum="100",
        )

        self.assertEqual(result.returncode, 1)
        self.assertIn("below required 100.00%", result.stderr)

    def test_makefile_quotes_configurable_python_executable(self) -> None:
        makefile = MAKEFILE.read_text(encoding="utf-8")

        self.assertNotIn("PYTHONPATH=src $(PYTHON)", makefile)
        self.assertNotIn("\n\t$(PYTHON) scripts/check_mutation_score.py", makefile)
        self.assertIn('MUTMUT ?= "$(PYTHON)" scripts/run_mutmut.py', makefile)
        self.assertIn("MUTMUT_RUN_ARGS ?= --max-children 4", makefile)
        self.assertIn('PYTHONPATH=src "$(PYTHON)"', makefile)
        self.assertIn('\n\t"$(PYTHON)" scripts/check_mutation_score.py', makefile)
        self.assertIn(
            "\n\tPYTHONDONTWRITEBYTECODE=1 $(MUTMUT) run",
            makefile,
        )
        self.assertIn("\n\t$(MUTMUT) export-cicd-stats", makefile)

    def test_mutmut_runner_pins_pyjwt_crypto_before_entering_mutmut(self) -> None:
        spec = importlib.util.spec_from_file_location("mgb_run_mutmut", MUTMUT_RUNNER)
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        runner = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(runner)
        events: list[str] = []
        fake_main = types.ModuleType("mutmut.__main__")
        fake_main.cli = lambda: events.append("cli")

        with (
            mock.patch.object(
                runner,
                "_pin_pyjwt_crypto",
                side_effect=lambda: events.append("pin"),
            ),
            mock.patch.dict(sys.modules, {"mutmut.__main__": fake_main}),
        ):
            runner.main()

        self.assertEqual(events, ["pin", "cli"])

    def test_development_toolchain_excludes_the_vulnerable_pytest_range(
        self,
    ) -> None:
        pyproject = PYPROJECT.read_text(encoding="utf-8")

        self.assertIn('"pytest>=9.0.3,<10"', pyproject)
        self.assertIn('"mutmut==3.8.1"', pyproject)
        self.assertIn('"pip-audit==2.10.1"', pyproject)

    def test_mutation_exclusions_do_not_hide_sql_or_raised_exceptions(self) -> None:
        configuration = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
        patterns = configuration["tool"]["mutmut"]["do_not_mutate_patterns"]

        self.assertEqual(
            patterns,
            [r"^\s*(?:field|subject|help|description|prog)\s*="],
        )

    def test_package_license_uses_spdx_with_supported_setuptools(self) -> None:
        configuration = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))

        with self.subTest("SPDX license expression"):
            self.assertEqual(configuration["project"]["license"], "MIT")
        with self.subTest("SPDX-capable build requirement"):
            self.assertEqual(
                configuration["build-system"]["requires"], ["setuptools>=77"]
            )

    def test_development_lock_is_hashed_and_auditable(self) -> None:
        lock = DEVELOPMENT_LOCK.read_text(encoding="utf-8")

        self.assertRegex(lock, r"(?m)^pytest==9\.[0-9.]+")
        self.assertIn("--hash=sha256:", lock)
        makefile = MAKEFILE.read_text(encoding="utf-8")
        self.assertIn("dependency-audit:", makefile)
        self.assertIn(
            '"$(PYTHON)" -m pip_audit -r requirements-development.lock',
            makefile,
        )

    def test_panopticon_has_a_final_runtime_and_test_catch_all(self) -> None:
        configuration = PANOPTICON.read_text(encoding="utf-8")

        self.assertIn("- src/magic_glass_box/**", configuration)
        self.assertIn("- tests/**", configuration)

    def test_official_code_gates_fail_when_git_is_unavailable(self) -> None:
        makefile = MAKEFILE.read_text(encoding="utf-8")

        self.assertIn(".PHONY: require-git", makefile)
        self.assertIn("test: require-git", makefile)
        self.assertIn("coverage: require-git", makefile)
        self.assertIn("candidate-test: require-git", makefile)
        self.assertIn("gate-test: require-git", makefile)
        self.assertIn("integration-test: require-git", makefile)

    def test_runtime_proof_image_contains_basic_repository_tools(self) -> None:
        dockerfile = RUNTIME_DOCKERFILE.read_text(encoding="utf-8")

        for package in ("git", "make", "patch"):
            with self.subTest(package=package):
                self.assertRegex(dockerfile, rf"(?m)^\s*{package}\s+\\$")
        self.assertIn("/usr/local/bin/apply_patch", dockerfile)


if __name__ == "__main__":
    unittest.main()
