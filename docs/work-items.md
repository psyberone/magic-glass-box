# Work-item import contract

Magic Glass Box accepts forge-neutral work through the versioned JSON contract
in [`schemas/work-item-v1.schema.json`](../schemas/work-item-v1.schema.json).
Import is the only external work-item operation: the control plane does not
fetch, export, synchronize, close, or otherwise mutate objects in a source
service.

## Identity and upsert behavior

The durable identity is the tuple `(project, source, external_id)`. An import is
atomic and reports one of three outcomes:

- `created` inserts the local item and immutable version 1;
- `updated` appends an immutable version when a newer source revision changes
  normalized content; or
- `unchanged` records a successful observation without appending duplicate
  content.

`source_updated_at` is normalized to UTC at construction and provides source
ordering. This invariant applies equally to the JSON parser and direct internal
construction, so storage never compares mixed-offset timestamp strings. An
older timestamp is rejected as stale. Different content at the same timestamp
is rejected as ambiguous rather than choosing a winner. Content may
legitimately return to an earlier digest after an intervening version.

Every new run must name a local work-item ID and pins its current immutable
version. A later import cannot change the title or instructions visible to that
run. Run listings expose the untrusted title and version as metadata; the full
brief is loaded only through the trusted execution projection.

Each work item also owns one active work-memory generation. An operator may
complete it only after its runs and assignments are terminal and a final
`outcome` has been recorded. Completion seals that generation without deleting
it. A later import may append content versions but does not implicitly reopen
the work. Explicit reopen creates a blank next generation, and only subsequent
runs pin it.

## Trust boundary

Every imported string is untrusted content. An envelope cannot choose its local
project, assignee, agent profile, credentials, network access, lifecycle state,
or execution policy. Those decisions are supplied separately by the local
operator. Unknown fields fail validation so an adapter cannot smuggle authority
through a provider-specific extension.

The contract intentionally contains no provider URL, raw issue payload, comment
history, transcript, credential, lifecycle command, or outbound action.
`workspace_path` context entries must be canonical relative POSIX paths; Magic
Glass Box never fetches a URL found in imported text.

## Adapter boundary

An adapter may authenticate to a source service outside Magic Glass Box and
write one JSON document matching the contract. The core consumes only that
document:

```console
some-adapter owner/repository 42 > work-item.json
magic-glass-box work-item import work-item.json \
  --project box-project \
  --imported-by operator
```

Re-running the same command is safe. The returned local ID can then be used to
create a run:

```console
magic-glass-box run create \
  --project box-project \
  --profile Worker \
  --work-item wki_<id> \
  --requested-by operator
```

Provider-specific adapters can live in separate packages or user-contributed
repositories. They need no privileged plugin interface inside the orchestrator.
