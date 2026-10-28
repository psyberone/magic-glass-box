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
from magic_glass_box.krab import (
    ActivityPage,
    InboxAck,
    InboxPage,
    KrabError,
    KrabIdentity,
    KrabProject,
    KrabSession,
    SessionCloseReceipt,
)
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
        self.coordinator_calls: list[
            tuple[str, tuple[object, ...], dict[str, object]]
        ] = []

    def whoami(self) -> KrabIdentity:
        return self.identity

    def projects(self) -> tuple[KrabProject, ...]:
        return self.visible_projects

    def activity(self, project_key: str, **kwargs: object) -> ActivityPage:
        self.activity_calls.append((project_key, kwargs))
        after = int(kwargs["after"])
        return ActivityPage(project_key, after, after, ())

    def sessions(self, project_key: str) -> tuple[KrabSession, ...]:
        self.coordinator_calls.append(("sessions", (project_key,), {}))
        return ()

    def create_session(self, project_key: str, **kwargs: object) -> KrabSession:
        self.coordinator_calls.append(("create_session", (project_key,), kwargs))
        return KrabSession(
            "ses_" + "3" * 32,
            project_key,
            "active",
            self.identity.user_id,
            "2026-10-27T12:00:00Z",
            None,
            "Room",
            (),
            (),
            PROJECT_ID,
        )

    def close_session(
        self, project_key: str, session_id: str, **kwargs: object
    ) -> SessionCloseReceipt:
        self.coordinator_calls.append(
            ("close_session", (project_key, session_id), kwargs)
        )
        return SessionCloseReceipt(
            session_id, project_key, "closed", "2026-10-27T13:00:00Z"
        )

    def inbox(self, project_key: str, **kwargs: object) -> InboxPage:
        self.coordinator_calls.append(("inbox", (project_key,), kwargs))
        return InboxPage(project_key, 0, 0, 0, bool(kwargs["unread"]), ())

    def ack_inbox(self, project_key: str, **kwargs: object) -> InboxAck:
        self.coordinator_calls.append(("ack_inbox", (project_key,), kwargs))
        return InboxAck(project_key, int(kwargs["through"]), "2026-10-27T14:00:00Z")

    def create_session_item(
        self, project_key: str, session_id: str, **kwargs: object
    ) -> object:
        self.coordinator_calls.append(
            ("create_session_item", (project_key, session_id), kwargs)
        )
        return {"kind": "sent"}

    def get_session_item(
        self, project_key: str, session_id: str, item_id: str
    ) -> object:
        self.coordinator_calls.append(
            ("get_session_item", (project_key, session_id, item_id), {})
        )
        return {"kind": "item"}

    def change_session_item_status(
        self, project_key: str, session_id: str, item_id: str, **kwargs: object
    ) -> object:
        self.coordinator_calls.append(
            (
                "change_session_item_status",
                (project_key, session_id, item_id),
                kwargs,
            )
        )
        return {"kind": "changed"}


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

    def test_coordinator_krab_hooks_use_the_bound_identity(self) -> None:
        self.control.add_krab_identity(
            instance_name="Local Krab", credential_path=self.token_path
        )
        auth = {
            "instance_name": "Local Krab",
            "krab_handle": "agent_one",
        }

        self.assertEqual(self.control.krab_projects(**auth), (self.project,))
        self.assertEqual(
            self.control.krab_sessions(project_key="box-project", **auth), ()
        )
        self.assertEqual(
            self.clients[-1].coordinator_calls,
            [("sessions", ("box-project",), {})],
        )

        session = self.control.create_krab_session(
            project_key="box-project",
            label="Room",
            participant_handles=("agent_one", "agent_two"),
            idempotency_key="mgb.session:1234",
            **auth,
        )
        self.assertEqual(session.untrusted_label, "Room")
        self.assertEqual(
            self.clients[-1].coordinator_calls,
            [
                (
                    "create_session",
                    ("box-project",),
                    {
                        "label": "Room",
                        "participant_handles": ("agent_one", "agent_two"),
                        "idempotency_key": "mgb.session:1234",
                    },
                )
            ],
        )

        closed = self.control.close_krab_session(
            project_key="box-project",
            session_id="ses_" + "3" * 32,
            idempotency_key="mgb.session:close-1234",
            **auth,
        )
        self.assertEqual(closed.state, "closed")

        page = self.control.krab_inbox(
            project_key="box-project",
            after=None,
            unread=True,
            limit=17,
            wait=9,
            **auth,
        )
        self.assertTrue(page.unread)
        self.assertEqual(
            self.clients[-1].coordinator_calls,
            [
                (
                    "inbox",
                    ("box-project",),
                    {"after": None, "unread": True, "limit": 17, "wait": 9},
                )
            ],
        )

        ack = self.control.ack_krab_inbox(
            project_key="box-project",
            through=7,
            idempotency_key="mgb.inbox:ack-1234",
            **auth,
        )
        self.assertEqual(ack.acked_through, 7)

        sent = self.control.send_krab_item(
            project_key="box-project",
            session_id="ses_" + "3" * 32,
            recipient_handle="agent_two",
            title="Please review",
            body="Private body",
            idempotency_key="mgb.item:send-1234",
            claim_id=None,
            completion_policy="workflow",
            **auth,
        )
        self.assertEqual(sent, {"kind": "sent"})

        shown = self.control.get_krab_item(
            project_key="box-project",
            session_id="ses_" + "3" * 32,
            item_id="itm_" + "4" * 32,
            **auth,
        )
        self.assertEqual(shown, {"kind": "item"})

        changed = self.control.change_krab_item_status(
            project_key="box-project",
            session_id="ses_" + "3" * 32,
            item_id="itm_" + "4" * 32,
            status="open.needs_changes",
            expected_version=1,
            note="Address findings",
            idempotency_key="mgb.item:status-1234",
            **auth,
        )
        self.assertEqual(changed, {"kind": "changed"})
        self.assertEqual(
            self.clients[-1].coordinator_calls,
            [
                (
                    "change_session_item_status",
                    (
                        "box-project",
                        "ses_" + "3" * 32,
                        "itm_" + "4" * 32,
                    ),
                    {
                        "status": "open.needs_changes",
                        "expected_version": 1,
                        "note": "Address findings",
                        "idempotency_key": "mgb.item:status-1234",
                    },
                )
            ],
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

    def test_service_operation_import_resolves_the_local_importer(self) -> None:
        database = mock.Mock()
        database.get_user.return_value = {"id": "boxu_operator"}
        database.import_work_item_from_service_operation.return_value = {
            "id": "wki_one",
            "import_status": "created",
        }
        control = ControlPlane(database)

        imported = control.import_work_item_from_service_operation(
            project_key="box-project",
            operation_id="sop_" + "1" * 32,
            imported_by_handle="operator",
        )

        self.assertEqual(imported["id"], "wki_one")
        database.get_user.assert_called_once_with("operator")
        database.import_work_item_from_service_operation.assert_called_once_with(
            project_key="box-project",
            operation_id="sop_" + "1" * 32,
            imported_by="boxu_operator",
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
