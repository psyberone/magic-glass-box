from __future__ import annotations

import os
import tempfile
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from typing import Any

from magic_glass_box.credentials import PrivateFileCredentialResolver
from magic_glass_box.db import (
    ConcurrentUpdateError,
    ConflictError,
    Database,
    NotFoundError,
)
from magic_glass_box.domain import (
    AgentProfileSpec,
    InteractionKind,
    ProjectContext,
    RunState,
    ValidationError,
    WorkItemEnvelope,
)
from magic_glass_box.krab import (
    KrabError,
    KrabIdentity,
    KrabUserRef,
    SessionItem,
    WorkClaimRef,
)
from magic_glass_box.service import AcceptedInteractionResponse, ControlPlane

AGENT_USER_ID = "usr_" + "1" * 32
REVIEWER_USER_ID = "usr_" + "2" * 32
PROJECT_ID = "prj_" + "3" * 32
SESSION_ID = "ses_" + "4" * 32
REQUEST_ITEM_ID = "itm_" + "5" * 32
RESPONSE_ITEM_ID = "itm_" + "6" * 32
CLAIM_ID = "clm_" + "7" * 32
AGENT_TOKEN = f"krab_{AGENT_USER_ID}_" + "A" * 43
REVIEWER_TOKEN = f"krab_{REVIEWER_USER_ID}_" + "B" * 43


def session_item(
    *,
    item_id: str,
    creator: KrabUserRef,
    recipient: KrabUserRef,
    title: str,
    body: str,
) -> SessionItem:
    return SessionItem(
        item_id=item_id,
        project_key="box-project",
        session_id=SESSION_ID,
        creator=creator,
        recipient=recipient,
        work_claim=None,
        status="open.pending",
        version=1,
        created_at="2026-10-09T12:00:00Z",
        updated_at="2026-10-09T12:00:00Z",
        event_seq=7 if item_id == REQUEST_ITEM_ID else None,
        untrusted_title=title,
        untrusted_body=body,
        content_risk_flags=("instruction_override",),
        history=(),
    )


class FakeInteractionClient:
    def __init__(self) -> None:
        self.identity = KrabIdentity(AGENT_USER_ID, "agent_one", 1)
        self.request_item = session_item(
            item_id=REQUEST_ITEM_ID,
            creator=KrabUserRef(AGENT_USER_ID, "agent_one"),
            recipient=KrabUserRef(REVIEWER_USER_ID, "reviewer"),
            title="Approval needed",
            body="SECRET QUESTION BODY",
        )
        self.response_item = session_item(
            item_id=RESPONSE_ITEM_ID,
            creator=KrabUserRef(REVIEWER_USER_ID, "reviewer"),
            recipient=KrabUserRef(AGENT_USER_ID, "agent_one"),
            title="Approval response",
            body="SECRET HUMAN RESPONSE",
        )
        self.create_calls: list[tuple[str, str, dict[str, Any]]] = []
        self.get_calls: list[tuple[str, str, str]] = []

    def whoami(self) -> KrabIdentity:
        return self.identity

    def create_session_item(
        self, project_key: str, session_id: str, **kwargs: Any
    ) -> SessionItem:
        self.create_calls.append((project_key, session_id, kwargs))
        return self.request_item

    def get_session_item(
        self, project_key: str, session_id: str, item_id: str
    ) -> SessionItem:
        self.get_calls.append((project_key, session_id, item_id))
        return self.response_item


class InteractionTransportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        os.chmod(self.root, 0o700)
        self.agent_token_path = self.root / "agent.token"
        self.agent_token_path.write_text(AGENT_TOKEN + "\n", encoding="ascii")
        os.chmod(self.agent_token_path, 0o600)
        self.reviewer_token_path = self.root / "reviewer.token"
        self.reviewer_token_path.write_text(REVIEWER_TOKEN + "\n", encoding="ascii")
        os.chmod(self.reviewer_token_path, 0o600)

        self.database = Database(self.root / "control-plane.db")
        self.database.initialize()
        self.operator = self.database.create_user("operator", "Local Operator")
        self.reviewer = self.database.create_user("reviewer", "Human Reviewer")
        self.instance = self.database.create_krab_instance(
            "Local Krab", "http://127.0.0.1:8765"
        )
        self.agent_identity = self.database.store_krab_identity(
            self.instance["id"],
            KrabIdentity(AGENT_USER_ID, "agent_one", 1),
            "file:" + str(self.agent_token_path),
        )
        self.reviewer_identity = self.database.store_krab_identity(
            self.instance["id"],
            KrabIdentity(REVIEWER_USER_ID, "reviewer", 1),
            "file:" + str(self.reviewer_token_path),
            user_id=self.reviewer["id"],
        )
        self.database.create_project(
            instance_id=self.instance["id"],
            krab_project_id=PROJECT_ID,
            krab_project_key="box-project",
            display_name="Magic Glass Box",
            context=ProjectContext.from_mapping(
                {"purpose": "Prove asynchronous interaction transport"}
            ),
            created_by=self.operator["id"],
        )
        self.work_item = self.database.import_work_item(
            project_key="box-project",
            envelope=WorkItemEnvelope.from_mapping(
                {
                    "schema_version": 1,
                    "source": "manual",
                    "external_id": "transport-test-item",
                    "source_updated_at": "2026-10-09T12:00:00Z",
                    "title": "Exercise interaction transport",
                    "brief": "Provide a pinned assignment for transport tests.",
                }
            ),
            imported_by=self.operator["id"],
        )
        self.database.create_agent_profile(
            name="Disposable Worker",
            spec=AgentProfileSpec.create(image="agent@sha256:abc", command=["agent"]),
            krab_identity_id=self.agent_identity["id"],
            created_by=self.operator["id"],
        )
        self.client = FakeInteractionClient()
        self.factory_calls: list[tuple[str, str]] = []

        def factory(base_url: str, token: str) -> Any:
            self.factory_calls.append((base_url, token))
            return self.client

        self.control = ControlPlane(
            self.database,
            credential_resolver=PrivateFileCredentialResolver(),
            client_factory=factory,
        )

    def running_run(
        self,
        *,
        session_id: str | None = SESSION_ID,
        claim_id: str | None = None,
    ) -> dict[str, Any]:
        run = self.database.create_run(
            project_key="box-project",
            profile_name="Disposable Worker",
            work_item_id=self.work_item["id"],
            requested_by=self.operator["id"],
            krab_session_id=session_id,
            krab_claim_id=claim_id,
        )
        self.database.transition_run(
            run["id"], target=RunState.PREPARING, expected_version=1
        )
        return self.database.transition_run(
            run["id"], target=RunState.RUNNING, expected_version=2
        )

    def open_interaction(self, run: dict[str, Any]) -> dict[str, Any]:
        return self.control.open_interaction(
            run["id"],
            kind=InteractionKind.APPROVAL,
            recipient_krab_handle="reviewer",
            title="Approval needed",
            body="SECRET QUESTION BODY",
            idempotency_key="mgb.request:1234",
            expected_run_version=run["version"],
        )

    def test_request_and_response_are_verified_correlated_and_not_persisted(
        self,
    ) -> None:
        run = self.running_run()

        opened = self.open_interaction(run)

        self.assertEqual(opened["request_item_id"], REQUEST_ITEM_ID)
        self.assertEqual(opened["state"], "open")
        self.assertEqual(opened["run_state"], "waiting_for_input")
        self.assertEqual(
            self.client.create_calls,
            [
                (
                    "box-project",
                    SESSION_ID,
                    {
                        "recipient_handle": "reviewer",
                        "title": "Approval needed",
                        "body": "SECRET QUESTION BODY",
                        "idempotency_key": "mgb.request:1234",
                    },
                )
            ],
        )
        database_bytes = b"".join(
            artifact.read_bytes() for artifact in self.root.glob("control-plane.db*")
        )
        self.assertNotIn(b"SECRET QUESTION BODY", database_bytes)

        accepted = self.control.accept_interaction_response(
            opened["id"],
            response_item_id=RESPONSE_ITEM_ID,
            expected_version=1,
        )

        self.assertEqual(
            accepted,
            AcceptedInteractionResponse(
                interaction_id=opened["id"],
                run_id=run["id"],
                response_item_id=RESPONSE_ITEM_ID,
                interaction_state="answered",
                interaction_version=2,
                run_state="queued",
                run_version=5,
                untrusted_title="Approval response",
                untrusted_body="SECRET HUMAN RESPONSE",
                content_risk_flags=("instruction_override",),
            ),
        )
        self.assertEqual(
            self.client.get_calls,
            [("box-project", SESSION_ID, RESPONSE_ITEM_ID)],
        )
        reread = self.control.read_interaction_response(run["id"], opened["id"])
        self.assertEqual(reread.untrusted_body, "SECRET HUMAN RESPONSE")
        self.assertEqual(reread.interaction_id, opened["id"])
        self.assertEqual(reread.run_id, run["id"])
        self.assertEqual(
            self.client.get_calls,
            [
                ("box-project", SESSION_ID, RESPONSE_ITEM_ID),
                ("box-project", SESSION_ID, RESPONSE_ITEM_ID),
            ],
        )
        self.assertEqual(
            self.factory_calls,
            [
                ("http://127.0.0.1:8765", AGENT_TOKEN),
                ("http://127.0.0.1:8765", AGENT_TOKEN),
                ("http://127.0.0.1:8765", AGENT_TOKEN),
            ],
        )
        database_bytes = b"".join(
            artifact.read_bytes() for artifact in self.root.glob("control-plane.db*")
        )
        self.assertNotIn(b"SECRET QUESTION BODY", database_bytes)
        self.assertNotIn(b"SECRET HUMAN RESPONSE", database_bytes)

    def test_response_read_is_bound_to_an_answered_interaction_and_run(self) -> None:
        run = self.running_run()
        opened = self.open_interaction(run)

        with self.assertRaisesRegex(ConflictError, "no accepted response"):
            self.control.read_interaction_response(run["id"], opened["id"])
        self.assertEqual(self.client.get_calls, [])

        self.control.accept_interaction_response(
            opened["id"], response_item_id=RESPONSE_ITEM_ID, expected_version=1
        )
        other = self.running_run(session_id="ses_" + "8" * 32)
        calls_before = list(self.client.get_calls)
        with self.assertRaisesRegex(NotFoundError, "not found for run"):
            self.control.read_interaction_response(other["id"], opened["id"])
        self.assertEqual(self.client.get_calls, calls_before)

    def test_request_metadata_must_match_the_pinned_run_and_recipient(self) -> None:
        run = self.running_run()
        original = self.client.request_item
        cases = (
            (
                replace(
                    original,
                    creator=KrabUserRef("usr_" + "9" * 32, "agent_nine"),
                ),
                "interaction item has an unexpected creator",
            ),
            (
                replace(
                    original,
                    recipient=KrabUserRef("usr_" + "8" * 32, "reviewer_eight"),
                ),
                "interaction item has an unexpected recipient",
            ),
            (
                replace(original, project_key="other-project"),
                "interaction item belongs to another project",
            ),
            (
                replace(original, session_id="ses_" + "7" * 32),
                "interaction item belongs to another session",
            ),
            (
                replace(original, status="open.in_progress"),
                "interaction request is not pending",
            ),
            (
                replace(original, version=2),
                "interaction request has an unexpected version",
            ),
            (
                replace(original, event_seq=None),
                "interaction request has no event sequence",
            ),
        )
        for candidate, message in cases:
            with self.subTest(message=message), self.assertRaises(KrabError) as caught:
                self.client.request_item = candidate
                self.open_interaction(run)
            self.assertEqual(str(caught.exception), message)
            current = self.database.get_run(run["id"])
            self.assertEqual((current["state"], current["version"]), ("running", 3))
            self.assertEqual(current["interactions"], [])
        self.client.request_item = original

    def test_response_metadata_must_reverse_the_verified_route(self) -> None:
        run = self.running_run()
        opened = self.open_interaction(run)
        original = self.client.response_item
        cases = (
            (
                replace(
                    original,
                    creator=KrabUserRef("usr_" + "9" * 32, "reviewer_nine"),
                ),
                "creator",
            ),
            (
                replace(
                    original,
                    recipient=KrabUserRef("usr_" + "8" * 32, "agent_eight"),
                ),
                "recipient",
            ),
            (replace(original, project_key="other-project"), "project"),
            (replace(original, session_id="ses_" + "7" * 32), "session"),
            (replace(original, item_id="itm_" + "7" * 32), "item"),
            (replace(original, status="closed.rejected"), "open"),
        )
        for candidate, message in cases:
            with (
                self.subTest(message=message),
                self.assertRaisesRegex(KrabError, message),
            ):
                self.client.response_item = candidate
                self.control.accept_interaction_response(
                    opened["id"],
                    response_item_id=RESPONSE_ITEM_ID,
                    expected_version=1,
                )
            current = self.database.get_interaction(opened["id"])
            self.assertEqual((current["state"], current["version"]), ("open", 1))
            self.assertEqual(current["run_state"], "waiting_for_input")
        self.client.response_item = original

    def test_preflight_rejects_stale_run_and_unbound_recipient_without_sending(
        self,
    ) -> None:
        run = self.running_run()
        with self.assertRaisesRegex(ConcurrentUpdateError, "run version"):
            self.control.open_interaction(
                run["id"],
                kind=InteractionKind.QUESTION,
                recipient_krab_handle="reviewer",
                title="Question",
                body="Body",
                idempotency_key="mgb.request:stale",
                expected_run_version=2,
            )
        self.assertEqual(self.client.create_calls, [])

        unbound = self.database.store_krab_identity(
            self.instance["id"],
            KrabIdentity("usr_" + "7" * 32, "observer", 1),
            "file:" + str(self.reviewer_token_path),
        )
        self.assertIsNone(unbound["local_user_id"])
        with self.assertRaisesRegex(NotFoundError, "bound local user"):
            self.control.open_interaction(
                run["id"],
                kind=InteractionKind.QUESTION,
                recipient_krab_handle="observer",
                title="Question",
                body="Body",
                idempotency_key="mgb.request:unbound",
                expected_run_version=run["version"],
            )
        self.assertEqual(self.client.create_calls, [])

    def test_request_preflight_rejects_invalid_inputs_state_and_inactive_user(
        self,
    ) -> None:
        run = self.running_run()
        for kind in ("approval", None):
            with (
                self.subTest(kind=kind),
                self.assertRaisesRegex(ValidationError, "kind"),
            ):
                self.control.open_interaction(
                    run["id"],
                    kind=kind,  # type: ignore[arg-type]
                    recipient_krab_handle="reviewer",
                    title="Approval needed",
                    body="Body",
                    idempotency_key="mgb.request:invalid-kind",
                    expected_run_version=run["version"],
                )
        for version in (True, 0):
            with (
                self.subTest(version=version),
                self.assertRaisesRegex(ValidationError, "expected_run_version"),
            ):
                self.control.open_interaction(
                    run["id"],
                    kind=InteractionKind.APPROVAL,
                    recipient_krab_handle="reviewer",
                    title="Approval needed",
                    body="Body",
                    idempotency_key="mgb.request:invalid-version",
                    expected_run_version=version,
                )

        queued = self.database.create_run(
            project_key="box-project",
            profile_name="Disposable Worker",
            work_item_id=self.work_item["id"],
            requested_by=self.operator["id"],
            krab_session_id="ses_" + "8" * 32,
        )
        with self.assertRaisesRegex(ConflictError, "not running"):
            self.control.open_interaction(
                queued["id"],
                kind=InteractionKind.QUESTION,
                recipient_krab_handle="reviewer",
                title="Question",
                body="Body",
                idempotency_key="mgb.request:queued",
                expected_run_version=queued["version"],
            )

        no_session = self.running_run(session_id=None)
        with self.assertRaisesRegex(ConflictError, "no Krab session"):
            self.control.open_interaction(
                no_session["id"],
                kind=InteractionKind.QUESTION,
                recipient_krab_handle="reviewer",
                title="Question",
                body="Body",
                idempotency_key="mgb.request:no-session",
                expected_run_version=no_session["version"],
            )

        with closing(self.database.connect()) as connection:
            connection.execute(
                "UPDATE users SET active = 0 WHERE id = ?", (self.reviewer["id"],)
            )
            connection.commit()
        with self.assertRaisesRegex(NotFoundError, "active bound local user"):
            self.open_interaction(run)
        self.assertEqual(self.client.create_calls, [])

    def test_claim_link_is_forwarded_and_verified(self) -> None:
        run = self.running_run(claim_id=CLAIM_ID)
        self.client.request_item = replace(
            self.client.request_item,
            work_claim=WorkClaimRef(CLAIM_ID, "issue", "owner/repository#42"),
        )

        opened = self.open_interaction(run)

        self.assertEqual(opened["request_item_id"], REQUEST_ITEM_ID)
        self.assertEqual(self.client.create_calls[0][2]["claim_id"], CLAIM_ID)

        other_claim = "clm_" + "9" * 32
        second_run = self.running_run(claim_id=CLAIM_ID)
        self.client.request_item = replace(
            self.client.request_item,
            work_claim=WorkClaimRef(other_claim, "pull_request", "owner/repository#43"),
        )
        with self.assertRaisesRegex(KrabError, "work-claim link"):
            self.open_interaction(second_run)
        self.assertEqual(self.database.get_run(second_run["id"])["interactions"], [])

    def test_response_preflight_rejects_invalid_stale_and_closed_without_reading(
        self,
    ) -> None:
        run = self.running_run()
        opened = self.open_interaction(run)

        for item_id, version, message in (
            ("itm_invalid", 1, "itm identifier"),
            (RESPONSE_ITEM_ID, True, "expected_version"),
            (RESPONSE_ITEM_ID, 0, "expected_version"),
            (RESPONSE_ITEM_ID, 2, "interaction version"),
        ):
            with (
                self.subTest(item_id=item_id, version=version),
                self.assertRaisesRegex(
                    (ValidationError, ConcurrentUpdateError), message
                ),
            ):
                self.control.accept_interaction_response(
                    opened["id"],
                    response_item_id=item_id,
                    expected_version=version,
                )
        self.assertEqual(self.client.get_calls, [])

        self.control.accept_interaction_response(
            opened["id"],
            response_item_id=RESPONSE_ITEM_ID,
            expected_version=1,
        )
        with self.assertRaisesRegex(ConflictError, "not open"):
            self.control.accept_interaction_response(
                opened["id"],
                response_item_id="itm_" + "8" * 32,
                expected_version=2,
            )
        self.assertEqual(len(self.client.get_calls), 1)

    def test_missing_run_fails_before_transport(self) -> None:
        with self.assertRaisesRegex(NotFoundError, "run not found"):
            self.control.open_interaction(
                "run_missing",
                kind=InteractionKind.QUESTION,
                recipient_krab_handle="reviewer",
                title="Question",
                body="Body",
                idempotency_key="mgb.request:missing-run",
                expected_run_version=1,
            )
        self.assertEqual(self.client.create_calls, [])


if __name__ == "__main__":
    unittest.main()
