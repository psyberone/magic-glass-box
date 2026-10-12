from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from magic_glass_box.credentials import PrivateFileCredentialResolver
from magic_glass_box.db import Database
from magic_glass_box.domain import (
    AgentProfileSpec,
    ProjectContext,
    RunState,
    SupervisionPolicy,
    WorkItemEnvelope,
)
from magic_glass_box.krab import ActivityPage, KrabError, KrabIdentity, KrabProject
from magic_glass_box.service import (
    ControlPlane,
    activity_to_dict,
    context_from_mapping,
    profile_from_mapping,
)

USER_ID = "usr_" + "1" * 32
PROJECT_ID = "prj_" + "2" * 32
TOKEN = f"krab_{USER_ID}_" + "A" * 43


class FakeKrabClient:
    def __init__(
        self,
        identity: KrabIdentity,
        projects: tuple[KrabProject, ...],
    ):
        self.identity = identity
        self.visible_projects = projects
        self.activity_calls: list[tuple[str, dict[str, object]]] = []

    def whoami(self) -> KrabIdentity:
        return self.identity

    def projects(self) -> tuple[KrabProject, ...]:
        return self.visible_projects

    def activity(self, project_key: str, **kwargs: object) -> ActivityPage:
        self.activity_calls.append((project_key, kwargs))
        after = int(kwargs["after"])
        return ActivityPage(project_key, after, after, ())


class ControlPlaneTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        os.chmod(self.root, 0o700)
        self.token_path = self.root / "krab.token"
        self.token_path.write_text(TOKEN + "\n", encoding="ascii")
        os.chmod(self.token_path, 0o600)
        self.database = Database(self.root / "control-plane.db")
        self.identity = KrabIdentity(USER_ID, "agent_one", 1)
        self.project = KrabProject(
            project_id=PROJECT_ID,
            project_key="box-project",
            role="member",
            label="Untrusted label",
            content_risk_flags=(),
        )
        self.factory_calls: list[tuple[str, str]] = []
        self.clients: list[FakeKrabClient] = []

        def factory(base_url: str, token: str) -> Any:
            self.factory_calls.append((base_url, token))
            client = FakeKrabClient(self.identity, (self.project,))
            self.clients.append(client)
            return client

        self.control = ControlPlane(
            self.database,
            credential_resolver=PrivateFileCredentialResolver(),
            client_factory=factory,
        )
        self.control.initialize()
        self.control.create_user("operator", "Local Operator")
        self.control.add_krab_instance("Local Krab", "http://localhost:8765")

    def test_identity_binding_persists_reference_not_secret(self) -> None:
        stored = self.control.add_krab_identity(
            instance_name="Local Krab",
            credential_path=self.token_path,
            local_user_handle="operator",
        )
        self.assertEqual(stored["krab_user_id"], USER_ID)
        self.assertEqual(
            stored["local_user_id"], self.database.get_user("operator")["id"]
        )
        self.assertEqual(stored["credential_ref"], "file:" + str(self.token_path))
        self.assertNotIn(TOKEN.encode("ascii"), self.database.path.read_bytes())
        self.assertEqual(self.factory_calls[0][1], TOKEN)

    def test_constructor_preserves_explicit_collaborators(self) -> None:
        resolver = PrivateFileCredentialResolver()

        def factory(_base_url: str, _token: str) -> Any:
            return FakeKrabClient(self.identity, ())

        control = ControlPlane(
            self.database,
            credential_resolver=resolver,
            client_factory=factory,
        )
        self.assertIs(control.database, self.database)
        self.assertIs(control.credential_resolver, resolver)
        self.assertIs(control.client_factory, factory)

    def test_user_display_name_defaults_to_handle(self) -> None:
        user = self.control.create_user("reviewer")
        self.assertEqual(user["handle"], "reviewer")
        self.assertEqual(user["display_name"], "reviewer")

    def test_project_registration_verifies_visibility_and_uses_trusted_name(
        self,
    ) -> None:
        self.control.add_krab_identity(
            instance_name="Local Krab", credential_path=self.token_path
        )
        project = self.control.register_project(
            instance_name="Local Krab",
            krab_handle="agent_one",
            project_key="box-project",
            display_name="Trusted Box Name",
            context=ProjectContext.from_mapping({"purpose": "Build Magic Glass Box"}),
            created_by_handle="operator",
        )
        self.assertEqual(project["krab_project_id"], PROJECT_ID)
        self.assertEqual(project["display_name"], "Trusted Box Name")
        self.assertNotEqual(project["display_name"], self.project.label)

    def test_project_registration_rejects_invisible_project(self) -> None:
        self.control.add_krab_identity(
            instance_name="Local Krab", credential_path=self.token_path
        )
        with self.assertRaisesRegex(KrabError, "cannot see"):
            self.control.register_project(
                instance_name="Local Krab",
                krab_handle="agent_one",
                project_key="other-project",
                display_name="Other",
                context=ProjectContext.from_mapping({"purpose": "Other"}),
                created_by_handle="operator",
            )

    def test_full_profile_context_and_run_workflow(self) -> None:
        self.control.add_krab_identity(
            instance_name="Local Krab", credential_path=self.token_path
        )
        self.control.register_project(
            instance_name="Local Krab",
            krab_handle="agent_one",
            project_key="box-project",
            display_name="Magic Glass Box",
            context=ProjectContext.from_mapping({"purpose": "Build Magic Glass Box"}),
            created_by_handle="operator",
        )
        revised = self.control.revise_project_context(
            project_key="box-project",
            context=ProjectContext.from_mapping(
                {"purpose": "Build Magic Glass Box safely"}
            ),
            created_by_handle="operator",
        )
        self.assertEqual(revised["context_version"], 2)
        spec = AgentProfileSpec.create(image="agent@sha256:one", command=["agent"])
        profile = self.control.create_agent_profile(
            name="Worker",
            instance_name="Local Krab",
            krab_handle="agent_one",
            spec=spec,
            created_by_handle="operator",
        )
        self.assertEqual(profile["profile_version"], 1)
        revised_profile = self.control.revise_agent_profile(
            name="Worker",
            instance_name="Local Krab",
            krab_handle="agent_one",
            spec=AgentProfileSpec.create(
                image="agent@sha256:two", command=["agent", "run"]
            ),
            created_by_handle="operator",
        )
        self.assertEqual(revised_profile["profile_version"], 2)
        work_item = self.control.import_work_item(
            project_key="box-project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "service-test-1",
                    "source_updated_at": "2026-10-09T12:00:00Z",
                    "title": "Exercise the control plane",
                    "brief": "Create a run from a pinned imported assignment.",
                }
            ),
            imported_by_handle="operator",
        )
        self.assertEqual(work_item["import_status"], "created")
        run = self.control.create_run(
            project_key="box-project",
            profile_name="Worker",
            work_item_id=work_item["id"],
            requested_by_handle="operator",
            source_revision="abc123",
            supervision_policy=SupervisionPolicy.from_mapping(
                {
                    "schema_version": 1,
                    "auto_execute": True,
                    "retry_backoff_seconds": [5, 30],
                    "interaction_escalation_seconds": [1800],
                    "attempt_idle_escalation_seconds": [],
                    "attempt_lease_expired_escalation_seconds": [0],
                    "other_attention_escalation_seconds": [],
                }
            ),
        )
        self.assertEqual(run["work_item_version"], 1)
        self.assertTrue(run["supervision_policy"]["auto_execute"])
        transitioned = self.control.transition_run(
            run["id"], target=RunState.PREPARING, expected_version=1
        )
        self.assertEqual(transitioned["state"], "preparing")
        self.assertEqual(list(self.database.iter_active_runs())[0]["id"], run["id"])

        queued = self.control.create_run(
            project_key="box-project",
            profile_name="Worker",
            work_item_id=work_item["id"],
            requested_by_handle="operator",
        )
        cancelled = self.control.request_run_cancellation(
            queued["id"],
            requested_by_handle="operator",
            expected_run_version=queued["version"],
            reason="No longer needed",
        )
        self.assertEqual(cancelled["state"], "cancelled")
        self.assertEqual(
            cancelled["events"][-1]["metadata"],
            {
                "reason": "No longer needed",
                "requested_by": self.database.get_user("operator")["id"],
            },
        )

    def test_activity_and_mapping_helpers(self) -> None:
        self.control.add_krab_identity(
            instance_name="Local Krab", credential_path=self.token_path
        )
        default_page = self.control.project_activity(
            instance_name="Local Krab",
            krab_handle="agent_one",
            project_key="box-project",
        )
        self.assertEqual(default_page, ActivityPage("box-project", 0, 0, ()))
        self.assertEqual(
            self.clients[-1].activity_calls,
            [("box-project", {"after": 0, "limit": 50, "wait": 0})],
        )
        page = self.control.project_activity(
            instance_name="Local Krab",
            krab_handle="agent_one",
            project_key="box-project",
            after=7,
            limit=23,
            wait=9,
        )
        self.assertEqual(activity_to_dict(page)["project_key"], "box-project")
        self.assertEqual(page.after, 7)
        self.assertEqual(
            self.clients[-1].activity_calls,
            [("box-project", {"after": 7, "limit": 23, "wait": 9})],
        )
        self.assertEqual(
            context_from_mapping({"purpose": "Magic Glass Box"}).purpose,
            "Magic Glass Box",
        )
        self.assertEqual(
            profile_from_mapping({"image": "agent", "command": ["run"]}).image,
            "agent",
        )

    def test_work_item_lifecycle_resolves_operator_handles(self) -> None:
        database = mock.Mock()
        database.get_user.return_value = {"id": "boxu_operator"}
        database.get_work_item.return_value = {"id": "wki_one"}
        database.complete_work_item.return_value = {"state": "completed"}
        database.reopen_work_item.return_value = {"state": "active"}
        control = ControlPlane(database)

        self.assertEqual(control.get_work_item("wki_one"), {"id": "wki_one"})
        self.assertEqual(
            control.complete_work_item(
                "wki_one",
                completed_by_handle="operator",
                expected_thread_version=4,
                reason="Verified.",
            ),
            {"state": "completed"},
        )
        self.assertEqual(
            control.reopen_work_item(
                "wki_one",
                reopened_by_handle="operator",
                expected_thread_version=5,
            ),
            {"state": "active"},
        )

        database.get_work_item.assert_called_once_with("wki_one")
        self.assertEqual(database.get_user.call_args_list, [mock.call("operator")] * 2)
        database.complete_work_item.assert_called_once_with(
            "wki_one",
            completed_by="boxu_operator",
            expected_thread_version=4,
            reason="Verified.",
        )
        database.reopen_work_item.assert_called_once_with(
            "wki_one",
            reopened_by="boxu_operator",
            expected_thread_version=5,
        )

    def test_authentication_rejects_credential_reassigned_to_another_user(self) -> None:
        self.control.add_krab_identity(
            instance_name="Local Krab", credential_path=self.token_path
        )
        self.identity = KrabIdentity("usr_" + "9" * 32, "agent_nine", 1)
        with self.assertRaisesRegex(KrabError, "different Krab identity"):
            self.control.project_activity(
                instance_name="Local Krab",
                krab_handle="agent_one",
                project_key="box-project",
            )


if __name__ == "__main__":
    unittest.main()
