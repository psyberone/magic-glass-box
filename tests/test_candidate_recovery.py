from __future__ import annotations

import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from magic_glass_box.candidate_recovery import (
    MAX_CANDIDATE_ANCESTORS,
    ensure_candidate_revision,
)
from magic_glass_box.candidates import (
    CANDIDATE_SCHEMA_VERSION,
    CandidateError,
    CandidateManifest,
)


def _manifest(
    index: int,
    *,
    base_revision: str,
    head_revision: str,
) -> CandidateManifest:
    digit = f"{index:032x}"
    return CandidateManifest(
        schema_version=CANDIDATE_SCHEMA_VERSION,
        attempt_id="att_" + digit,
        attempt_result_id="rsl_" + digit,
        producer_run_id="run_" + digit,
        project_id="boxp_" + "1" * 32,
        context_version_id="ctx_" + "2" * 32,
        work_thread_id="wth_" + "3" * 32,
        base_revision=base_revision,
        result_content_sha256=f"{index:064x}",
        head_revision=head_revision,
        tree_revision=f"{index + 100:040x}",
        object_format="sha1",
    )


def _missing_store() -> mock.Mock:
    store = mock.Mock()
    store.inspect.side_effect = CandidateError("candidate is absent")
    return store


class CandidateRecoveryTests(unittest.TestCase):
    def test_recovers_parent_before_child_and_verifies_each_result_digest(self) -> None:
        parent = _manifest(2, base_revision="a" * 40, head_revision="b" * 40)
        child = _manifest(
            1,
            base_revision=parent.head_revision,
            head_revision="c" * 40,
        )
        parent_result = SimpleNamespace(source_revision=parent.base_revision)
        child_result = SimpleNamespace(source_revision=child.base_revision)
        database = mock.Mock()
        database.get_candidate_source.side_effect = {
            child.attempt_id: {
                "change_policy_id": "gpl_child",
                "change_candidate_id": "cnd_parent",
                "result": child_result,
            },
            parent.attempt_id: {
                "change_policy_id": None,
                "change_candidate_id": None,
                "result": parent_result,
            },
        }.__getitem__
        database.get_gate_revision.return_value = {
            "manifest": parent.to_dict(),
            "policy": {"candidate_id": "cnd_parent"},
            "repository_path": "/repo",
        }
        results = mock.Mock()
        results.load_bundle.side_effect = (
            (parent_result, {}),
            (child_result, {}),
        )
        candidates = _missing_store()

        ensure_candidate_revision(
            database,
            results,
            candidates,
            child,
            Path("/repo"),
        )

        self.assertEqual(
            results.load_bundle.call_args_list,
            [
                mock.call(
                    parent.attempt_id,
                    expected_digest=parent.result_content_sha256,
                ),
                mock.call(
                    child.attempt_id,
                    expected_digest=child.result_content_sha256,
                ),
            ],
        )
        self.assertEqual(
            [call.args[0] for call in candidates.recover_revision.call_args_list],
            [parent, child],
        )

    def test_rejects_each_mismatched_parent_identity_field(self) -> None:
        parent = _manifest(2, base_revision="a" * 40, head_revision="b" * 40)
        child = _manifest(
            1,
            base_revision=parent.head_revision,
            head_revision="c" * 40,
        )
        cases = {
            "candidate": (parent, "cnd_other"),
            "project": (replace(parent, project_id="boxp_" + "4" * 32), "cnd_parent"),
            "context": (
                replace(parent, context_version_id="ctx_" + "5" * 32),
                "cnd_parent",
            ),
            "thread": (
                replace(parent, work_thread_id="wth_" + "6" * 32),
                "cnd_parent",
            ),
            "head": (
                replace(parent, head_revision="d" * 40),
                "cnd_parent",
            ),
        }
        for name, (recorded_parent, policy_candidate_id) in cases.items():
            with self.subTest(name=name):
                database = mock.Mock()
                database.get_candidate_source.return_value = {
                    "change_policy_id": "gpl_child",
                    "change_candidate_id": "cnd_parent",
                    "result": SimpleNamespace(source_revision=child.base_revision),
                }
                database.get_gate_revision.return_value = {
                    "manifest": recorded_parent.to_dict(),
                    "policy": {"candidate_id": policy_candidate_id},
                    "repository_path": "/repo",
                }

                with self.assertRaises(CandidateError):
                    ensure_candidate_revision(
                        database,
                        mock.Mock(),
                        _missing_store(),
                        child,
                        Path("/repo"),
                    )

    def test_rejects_a_cycle_before_attempting_result_recovery(self) -> None:
        revision = "a" * 40
        candidate = _manifest(1, base_revision=revision, head_revision=revision)
        database = mock.Mock()
        database.get_candidate_source.return_value = {
            "change_policy_id": "gpl_cycle",
            "change_candidate_id": "cnd_cycle",
            "result": SimpleNamespace(source_revision=revision),
        }
        database.get_gate_revision.return_value = {
            "manifest": candidate.to_dict(),
            "policy": {"candidate_id": "cnd_cycle"},
            "repository_path": "/repo",
        }
        results = mock.Mock()

        with self.assertRaises(CandidateError):
            ensure_candidate_revision(
                database,
                results,
                _missing_store(),
                candidate,
                Path("/repo"),
            )

        results.load_bundle.assert_not_called()

    def test_rejects_a_missing_parent_recovery_repository(self) -> None:
        parent = _manifest(2, base_revision="a" * 40, head_revision="b" * 40)
        child = _manifest(
            1,
            base_revision=parent.head_revision,
            head_revision="c" * 40,
        )
        database = mock.Mock()
        database.get_candidate_source.return_value = {
            "change_policy_id": "gpl_child",
            "change_candidate_id": "cnd_parent",
            "result": SimpleNamespace(source_revision=child.base_revision),
        }
        database.get_gate_revision.return_value = {
            "manifest": parent.to_dict(),
            "policy": {"candidate_id": "cnd_parent"},
            "repository_path": None,
        }

        with self.assertRaises(CandidateError):
            ensure_candidate_revision(
                database,
                mock.Mock(),
                _missing_store(),
                child,
                Path("/repo"),
            )

    def test_rejects_ancestry_deeper_than_the_bounded_limit(self) -> None:
        count = MAX_CANDIDATE_ANCESTORS + 1
        heads = [f"{index + 1:040x}" for index in range(count)]
        manifests = [
            _manifest(
                index + 1,
                base_revision=(heads[index + 1] if index + 1 < count else "f" * 40),
                head_revision=heads[index],
            )
            for index in range(count)
        ]
        sources: dict[str, dict[str, object]] = {}
        revisions: dict[str, dict[str, object]] = {}
        for index, manifest in enumerate(manifests):
            policy_id = f"gpl_{index}"
            sources[manifest.attempt_id] = {
                "change_policy_id": policy_id,
                "change_candidate_id": f"cnd_{index}",
                "result": SimpleNamespace(source_revision=manifest.base_revision),
            }
            if index + 1 < count:
                revisions[policy_id] = {
                    "manifest": manifests[index + 1].to_dict(),
                    "policy": {"candidate_id": f"cnd_{index}"},
                    "repository_path": "/repo",
                }
        database = mock.Mock()
        database.get_candidate_source.side_effect = sources.__getitem__
        database.get_gate_revision.side_effect = revisions.__getitem__
        results = mock.Mock()

        with self.assertRaises(CandidateError):
            ensure_candidate_revision(
                database,
                results,
                _missing_store(),
                manifests[0],
                Path("/repo"),
            )

        results.load_bundle.assert_not_called()
