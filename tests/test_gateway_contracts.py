from __future__ import annotations

import json
import unittest
from unittest import mock

from magic_glass_box.domain import GatewayCapability
from magic_glass_box.gateway_contracts import (
    GATEWAY_CATALOG_OPERATION,
    GATEWAY_CATALOG_VERSION,
    GATEWAY_OPERATION_CONTRACTS,
    MAX_GATEWAY_CATALOG_BYTES,
    FieldProvenance,
    GatewayContractError,
    OperationContract,
    TrustClass,
    build_gateway_catalog,
    response_field_provenance,
    validate_result_shape,
)


class GatewayContractTests(unittest.TestCase):
    def test_catalog_is_complete_bounded_and_scoped_to_granted_operations(self) -> None:
        granted = (
            GatewayCapability.RUN_DESCRIBE,
            GatewayCapability.INTERACTION_OPEN,
            GatewayCapability.INTERACTION_READ,
            GatewayCapability.HANDOFF_SUBMIT,
        )

        catalog = build_gateway_catalog(
            granted,
            requested_version=GATEWAY_CATALOG_VERSION,
        )

        self.assertEqual(set(GATEWAY_OPERATION_CONTRACTS), set(GatewayCapability))
        self.assertEqual(catalog["catalog_version"], GATEWAY_CATALOG_VERSION)
        self.assertEqual(catalog["meta_operation"], GATEWAY_CATALOG_OPERATION)
        self.assertEqual(
            [item["operation"] for item in catalog["operations"]],
            sorted(capability.value for capability in granted),
        )
        self.assertNotIn(
            GatewayCapability.PROVIDER_INVOKE.value,
            json.dumps(catalog, sort_keys=True),
        )
        self.assertLessEqual(
            len(json.dumps(catalog, separators=(",", ":"), sort_keys=True).encode()),
            MAX_GATEWAY_CATALOG_BYTES,
        )

        operations = {item["operation"]: item for item in catalog["operations"]}
        interaction = operations[GatewayCapability.INTERACTION_OPEN.value]
        self.assertFalse(interaction["request_schema"]["additionalProperties"])
        self.assertEqual(
            set(interaction["request_schema"]["required"]),
            {
                "body",
                "idempotency_key",
                "kind",
                "recipient_handle",
                "title",
            },
        )
        self.assertEqual(
            interaction["request_schema"]["properties"]["body"]["x-max-utf8-bytes"],
            16384,
        )
        handoff = operations[GatewayCapability.HANDOFF_SUBMIT.value]
        self.assertEqual(
            handoff["request_schema"],
            {"$ref": "schemas/handoff-v1.schema.json"},
        )
        self.assertEqual(
            catalog["schema_documents"]["schemas/handoff-v1.schema.json"]["$id"],
            "urn:magic-glass-box:schema:handoff:v1",
        )
        self.assertNotIn(
            "schemas/inference-invocation-request-v1.schema.json",
            catalog["schema_documents"],
        )
        self.assertEqual(
            handoff["follow_up_actions"],
            ["exit"],
        )

    def test_catalog_rejects_unknown_versions_and_oversized_output(self) -> None:
        with self.assertRaisesRegex(GatewayContractError, "catalog version"):
            build_gateway_catalog(
                (GatewayCapability.RUN_DESCRIBE,),
                requested_version=GATEWAY_CATALOG_VERSION + 1,
            )

        with (
            mock.patch(
                "magic_glass_box.gateway_contracts.MAX_GATEWAY_CATALOG_BYTES", 8
            ),
            self.assertRaisesRegex(GatewayContractError, "size limit"),
        ):
            build_gateway_catalog(
                (GatewayCapability.RUN_DESCRIBE,),
                requested_version=GATEWAY_CATALOG_VERSION,
            )

        with self.assertRaises(GatewayContractError):
            build_gateway_catalog(
                ("not-a-capability",),  # type: ignore[arg-type]
                requested_version=GATEWAY_CATALOG_VERSION,
            )

        with self.assertRaisesRegex(GatewayContractError, "unique"):
            build_gateway_catalog(
                (
                    GatewayCapability.RUN_DESCRIBE,
                    GatewayCapability.RUN_DESCRIBE,
                ),
                requested_version=GATEWAY_CATALOG_VERSION,
            )

    def test_catalog_accepts_its_exact_byte_limit_and_finds_nested_references(
        self,
    ) -> None:
        contract = OperationContract(
            capability=GatewayCapability.RUN_DESCRIBE,
            handler_name="_describe",
            request_fields=frozenset(),
            request_schema={"allOf": [{"$ref": "schemas/handoff-v1.schema.json"}]},
            result_schema={
                "properties": {
                    "nested": {
                        "oneOf": [{"$ref": "schemas/work-log-entry-v1.schema.json"}]
                    }
                }
            },
            follow_up_actions=(),
            response_provenance=(
                FieldProvenance(
                    "/result",
                    TrustClass.CONTROL_METADATA,
                    "control_plane",
                ),
            ),
        )
        with (
            mock.patch(
                "magic_glass_box.gateway_contracts.operation_contract",
                return_value=contract,
            ),
            mock.patch(
                "magic_glass_box.gateway_contracts._load_schema_document",
                side_effect=lambda reference: {"reference": reference},
            ) as load,
            mock.patch(
                "magic_glass_box.gateway_contracts.MAX_GATEWAY_CATALOG_BYTES", 8
            ),
            mock.patch(
                "magic_glass_box.gateway_contracts.canonical_json",
                return_value="12345678",
            ),
        ):
            catalog = build_gateway_catalog(
                (GatewayCapability.RUN_DESCRIBE,),
                requested_version=GATEWAY_CATALOG_VERSION,
            )

        self.assertEqual(
            catalog["schema_documents"],
            {
                "schemas/handoff-v1.schema.json": {
                    "reference": "schemas/handoff-v1.schema.json"
                },
                "schemas/work-log-entry-v1.schema.json": {
                    "reference": "schemas/work-log-entry-v1.schema.json"
                },
            },
        )
        self.assertEqual(
            [call.args[0] for call in load.call_args_list],
            [
                "schemas/handoff-v1.schema.json",
                "schemas/work-log-entry-v1.schema.json",
            ],
        )

    def test_response_provenance_labels_only_returned_content(self) -> None:
        result = {
            "run_id": "run_" + "1" * 32,
            "authority": {"gateway_capabilities": ["run.describe"]},
            "context": {"purpose": "Untrusted project purpose"},
            "work_item": None,
            "handoff": None,
        }

        labels = response_field_provenance(
            GatewayCapability.RUN_DESCRIBE,
            result,
        )

        by_path = {item["path"]: item for item in labels}
        self.assertEqual(
            by_path["/result/authority"]["trust_class"],
            TrustClass.AUTHORITY.value,
        )
        self.assertEqual(
            by_path["/result/context"],
            {
                "path": "/result/context",
                "source": "project_context",
                "trust_class": TrustClass.UNTRUSTED_CONTENT.value,
            },
        )
        self.assertNotIn("/result/changes_requested", by_path)

    def test_result_shape_validation_uses_the_published_contract(self) -> None:
        validate_result_shape(
            GatewayCapability.WORK_APPEND,
            {
                "content_sha256": "a" * 64,
                "entry_id": "wle_" + "1" * 32,
                "sequence": 1,
            },
        )

        with self.assertRaisesRegex(GatewayContractError, "result shape"):
            validate_result_shape(
                GatewayCapability.WORK_APPEND,
                {
                    "content_sha256": "a" * 64,
                    "entry_id": "wle_" + "1" * 32,
                },
            )

        with self.assertRaises(GatewayContractError):
            validate_result_shape(
                GatewayCapability.WORK_APPEND,
                ["content_sha256", "entry_id", "sequence"],
            )


if __name__ == "__main__":
    unittest.main()
