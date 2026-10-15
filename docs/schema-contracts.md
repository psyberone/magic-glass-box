# Published JSON Schema boundary

The files in `schemas/` are portable Draft 2020-12 structural contracts. They
describe object shape, required fields, scalar types, enums, item counts, and
the path and identity constraints that standard JSON Schema can express.
Examples in `examples/` must pass both those schemas and the corresponding
runtime parser.

Schema validation is useful adapter-side preflight, but it is not an acceptance
decision. Magic Glass Box's runtime parser is authoritative and additionally
enforces invariants that portable JSON Schema cannot express, including:

- UTF-8 byte limits rather than Unicode code-point counts;
- uniqueness by one property of an object, such as artifact path;
- strictly increasing supervision schedules;
- relationships between fields and records; and
- repository-relative symlink-graph containment.

An input that passes a published schema may therefore still be rejected by the
runtime semantic validator. The conformance tests deliberately cover both the
shared structural surface and these documented runtime-only guards; they do not
claim that two different validation languages accept identical input sets.
