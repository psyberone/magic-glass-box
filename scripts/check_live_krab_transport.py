from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import threading
from pathlib import Path


def parse_args() -> argparse.Namespace:
    repository = Path(__file__).resolve().parents[1]
    default_source = repository.parent / "imitation-krab" / "src"
    parser = argparse.ArgumentParser(
        description="Exercise interaction transport against a live imitation-krab API"
    )
    parser.add_argument(
        "--krab-source",
        type=Path,
        default=default_source,
        help="path to the imitation-krab src directory",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    krab_source = args.krab_source.expanduser().resolve()
    if not (krab_source / "imitation_krab" / "__init__.py").is_file():
        raise RuntimeError(f"imitation-krab source not found: {krab_source}")
    sys.path.insert(0, str(krab_source))

    from imitation_krab.db import Database as KrabDatabase
    from imitation_krab.http_api import make_server
    from imitation_krab.service import Service as KrabService

    from magic_glass_box.credentials import PrivateFileCredentialResolver
    from magic_glass_box.db import Database
    from magic_glass_box.domain import (
        AgentProfileSpec,
        InteractionKind,
        ProjectContext,
        RunState,
        WorkItemEnvelope,
    )
    from magic_glass_box.krab import KrabClient, KrabIdentity
    from magic_glass_box.service import ControlPlane

    with tempfile.TemporaryDirectory(prefix="mgb-live-krab-") as temporary:
        root = Path(temporary)
        krab_database = KrabDatabase(root / "krab.db", root / "pepper.key")
        krab_database.initialize()
        agent_user, agent_token = krab_database.create_user("agent_one")
        reviewer_user, reviewer_token = krab_database.create_user("reviewer")
        krab_project = krab_database.create_project(
            "box-project", "Magic Glass Box live transport"
        )
        krab_database.add_project_member("box-project", "agent_one", "coordinator")
        krab_database.add_project_member("box-project", "reviewer", "member")
        agent_actor = krab_database.authenticate(agent_token)
        if agent_actor is None:
            raise RuntimeError("live agent token did not authenticate")
        claim = (
            KrabService(krab_database)
            .create_work_claim(
                agent_actor,
                "box-project",
                "issue",
                {"external_id": "owner/repository#42"},
                "mgb.live.claim:0001",
            )
            .payload["trusted_metadata"]
        )
        session = (
            KrabService(krab_database)
            .create_session(
                agent_actor,
                "box-project",
                {
                    "label": "Interaction transport",
                    "participants": ["agent_one", "reviewer"],
                },
                "mgb.live.session:0001",
            )
            .payload["trusted_metadata"]["session_id"]
        )

        server = make_server(krab_database, port=0)
        port = int(server.server_address[1])
        server_thread = threading.Thread(
            target=server.serve_forever,
            kwargs={"poll_interval": 0.05},
            daemon=True,
        )
        server_thread.start()
        base_url = f"http://127.0.0.1:{port}"
        try:
            token_directory = root / "tokens"
            token_directory.mkdir(mode=0o700)
            agent_token_path = token_directory / "agent.token"
            agent_token_path.write_text(agent_token + "\n", encoding="ascii")
            os.chmod(agent_token_path, 0o600)
            reviewer_token_path = token_directory / "reviewer.token"
            reviewer_token_path.write_text(reviewer_token + "\n", encoding="ascii")
            os.chmod(reviewer_token_path, 0o600)

            database = Database(root / "control-plane.db")
            database.initialize()
            operator = database.create_user("operator", "Local Operator")
            reviewer = database.create_user("reviewer", "Human Reviewer")
            instance = database.create_krab_instance("Live Krab", base_url)
            agent_identity = database.store_krab_identity(
                instance["id"],
                KrabIdentity(agent_user["id"], agent_user["handle"], 1),
                "file:" + str(agent_token_path),
            )
            database.store_krab_identity(
                instance["id"],
                KrabIdentity(reviewer_user["id"], reviewer_user["handle"], 1),
                "file:" + str(reviewer_token_path),
                user_id=reviewer["id"],
            )
            database.create_project(
                instance_id=instance["id"],
                krab_project_id=krab_project["id"],
                krab_project_key=krab_project["key"],
                display_name="Magic Glass Box",
                context=ProjectContext.from_mapping(
                    {"purpose": "Live interaction transport proof"}
                ),
                created_by=operator["id"],
            )
            work_item = database.import_work_item(
                project_key="box-project",
                envelope=WorkItemEnvelope.from_mapping(
                    {
                        "schema_version": 1,
                        "source": "manual",
                        "external_id": "owner/repository#42",
                        "source_updated_at": "2026-10-09T12:00:00Z",
                        "title": "Exercise live interaction transport",
                        "brief": "Prove private question and response routing.",
                    }
                ),
                imported_by=operator["id"],
            )
            database.create_agent_profile(
                name="Live Worker",
                spec=AgentProfileSpec.create(
                    image="agent@sha256:live", command=["agent"]
                ),
                krab_identity_id=agent_identity["id"],
                created_by=operator["id"],
            )
            run = database.create_run(
                project_key="box-project",
                profile_name="Live Worker",
                work_item_id=work_item["id"],
                requested_by=operator["id"],
                krab_session_id=session,
                krab_claim_id=claim["claim_id"],
            )
            database.transition_run(
                run["id"], target=RunState.PREPARING, expected_version=1
            )
            run = database.transition_run(
                run["id"], target=RunState.RUNNING, expected_version=2
            )

            control = ControlPlane(
                database, credential_resolver=PrivateFileCredentialResolver()
            )
            request_title = "Live approval request"
            request_body = "LIVE PRIVATE QUESTION"
            request_key = "mgb.live.request:0001"
            interaction = control.open_interaction(
                run["id"],
                kind=InteractionKind.APPROVAL,
                recipient_krab_handle="reviewer",
                title=request_title,
                body=request_body,
                idempotency_key=request_key,
                expected_run_version=run["version"],
            )

            replayed = KrabClient(base_url, agent_token).create_session_item(
                "box-project",
                session,
                recipient_handle="reviewer",
                title=request_title,
                body=request_body,
                idempotency_key=request_key,
                claim_id=claim["claim_id"],
            )
            if replayed.item_id != interaction["request_item_id"]:
                raise RuntimeError("Krab idempotency replay returned another item")

            response_body = "LIVE PRIVATE HUMAN RESPONSE"
            response = KrabClient(base_url, reviewer_token).create_session_item(
                "box-project",
                session,
                recipient_handle="agent_one",
                title="Live approval response",
                body=response_body,
                idempotency_key="mgb.live.response:0001",
            )
            accepted = control.accept_interaction_response(
                interaction["id"],
                response_item_id=response.item_id,
                expected_version=interaction["version"],
            )
            if accepted.untrusted_body != response_body:
                raise RuntimeError("accepted response body did not round-trip")

            control_plane_bytes = b"".join(
                artifact.read_bytes()
                for artifact in root.glob("control-plane.db*")
                if artifact.is_file()
            )
            for private_text in (request_body, response_body):
                if private_text.encode("utf-8") in control_plane_bytes:
                    raise RuntimeError("private interaction content entered MGB state")

            room = control.control_room_snapshot(refresh_krab=True)
            roles = room["krab"]["roles"]
            if not any(
                role["krab_handle"] == "agent_one" and role["role"] == "coordinator"
                for role in roles
            ):
                raise RuntimeError("control room did not project the live Krab role")
            visible_claim = next(
                (
                    current
                    for current in room["krab"]["work_claims"]
                    if current["claim_id"] == claim["claim_id"]
                ),
                None,
            )
            if visible_claim is None:
                raise RuntimeError("control room did not project the live claim queue")
            if visible_claim["local_work_item"] != {
                "id": work_item["id"],
                "untrusted_title": "Exercise live interaction transport",
            }:
                raise RuntimeError("live claim was not linked to its local title")
            room_json = json.dumps(room, sort_keys=True)
            if request_body in room_json or response_body in room_json:
                raise RuntimeError("private interaction content entered control room")

            print(
                json.dumps(
                    {
                        "interaction_id": accepted.interaction_id,
                        "request_item_id": interaction["request_item_id"],
                        "response_item_id": accepted.response_item_id,
                        "interaction_state": accepted.interaction_state,
                        "run_state": accepted.run_state,
                        "idempotency_replay": "same_item",
                        "private_content_persisted_by_mgb": False,
                        "control_room_role": "coordinator",
                        "control_room_claim_id": claim["claim_id"],
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
        finally:
            server.shutdown()
            server.server_close()
            server_thread.join(timeout=2)
            if server_thread.is_alive():
                raise RuntimeError("live Krab server did not stop")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
