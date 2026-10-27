# Work-item import contract

Magic Glass Box accepts provider-neutral work through the versioned JSON
contract in
[`schemas/work-item-v1.schema.json`](../schemas/work-item-v1.schema.json).
An operator may import either a local document or the normalized result of one
explicitly authorized `work.get` service operation. A read never imports,
exports, synchronizes, closes, or otherwise mutates work by itself.

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

Out-of-process service providers use the same core-owned result contracts:

- `work.get` v1 returns one canonical work-item v1 envelope; and
- `work.list` v1 returns one discovery window matching
  [`work-item-list-result-v1.schema.json`](../schemas/work-item-list-result-v1.schema.json).

The list result contains at most 100 bounded references and a `truncated`
indicator. It deliberately excludes provider cursors, status names, queries,
raw descriptions, comments, and URLs. Listing is discovery only and cannot
create or update a durable item.

After a project-bound `work.get` operation succeeds, import its result
deliberately:

```console
magic-glass-box work-item import-operation sop_<id> \
  --project box-project \
  --imported-by operator
```

The command requires the operation's immutable capability binding to name that
exact project and accepts only a successful `work.get` v1 read. It runs the
ordinary atomic upsert and appends an immutable link to the exact service
operation attempt and receipt digest. Repeating the same operation is a true
no-op. Unbinding `work.get` prevents new project-routed reads without erasing or
invalidating a result that was already captured under valid authority.
