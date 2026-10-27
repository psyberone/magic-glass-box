# Jira reference provider

**Status: Milestone 14.4 deterministic mixed-provider proof complete; live
external execution pending explicit configuration.**

The separate `magic-glass-box-jira-provider` process is the first reference
provider for a non-repository resource. It owns one operator-selected Jira
Cloud base URL, account, API token, and private core-state mapping. MGB sees
only the service-sidecar v2 `work_project` authority and a one-way credential
scope fingerprint. Workers see neither.

The initial surface is deliberately small:

| Capability | Mode | Contract |
| --- | --- | --- |
| `work.list` v1 | read | Return at most 100 normalized references from one mapped project |
| `work.get` v1 | read | Return one exact issue as an import-ready work-item envelope |
| `work.state` v1 | effect | Idempotently observe or move one exact issue to the core-owned `done` state |

The carrier does not expose arbitrary JQL, raw HTTP, comments, Jira status
names, transition IDs, provider cursors, or response documents. It uses
Atlassian's enhanced [`/rest/api/3/search/jql`](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-search/),
exact [issue read](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issues/),
and issue-transition endpoints internally. The list query is constructed by
the provider from the authorized project and a provider-neutral limit; its
Jira page token becomes only the normalized `truncated` flag.

## Private credential file

Create an absolute, user-owned mode-`0600` JSON file outside the MGB state
directory and every worker mount:

```json
{
  "schema_version": 1,
  "base_url": "https://example.atlassian.net",
  "account": "operator@example.test",
  "api_token": "replace-with-a-Jira-API-token",
  "state_targets": {
    "ABC": {
      "done": "10001"
    }
  }
}
```

`state_targets` is optional when the provider advertises only reads. When
`work.state` is advertised, each mapped project must map `done` to its Jira
status ID. That provider-native ID stays in this private file. The provider
discovers the one currently available Jira transition whose destination is
that status; no transition ID enters an MGB operation.

Jira Cloud script authentication uses the account plus API token selected by
the operator. The token inherits that account's Jira permissions, so use a
dedicated account or otherwise constrain it appropriately. See Atlassian's
[security overview](https://developer.atlassian.com/cloud/jira/platform/security-overview/).

Inspect the file locally before registering authority:

```console
chmod 600 /absolute/private/jira-credential.json
magic-glass-box-jira-provider inspect \
  --credential-file /absolute/private/jira-credential.json
```

The command performs no Jira call. It prints only the configured host,
account, and a one-way credential-scope fingerprint. Copy those values into
[`examples/jira-service-sidecar.json`](../examples/jira-service-sidecar.json)
and replace the example deployment digest, project, and socket path.

## Registration and execution

```console
magic-glass-box service-sidecar register jira-main \
  --spec /absolute/private/jira-service-sidecar.json \
  --created-by operator
magic-glass-box capability bind demo-project work.list --version 1 \
  --sidecar jira-main --mapping primary --bound-by operator
magic-glass-box capability bind demo-project work.get --version 1 \
  --sidecar jira-main --mapping primary --bound-by operator
magic-glass-box capability bind demo-project work.state --version 1 \
  --sidecar jira-main --mapping primary --bound-by operator

magic-glass-box-jira-provider serve \
  --credential-file /absolute/private/jira-credential.json \
  --spec /absolute/private/jira-service-sidecar.json \
  --socket-path /absolute/private/jira.sock \
  --max-invocations 100
```

Reference operation documents are:

- [`jira-work-list.json`](../examples/jira-work-list.json)
- [`jira-work-get.json`](../examples/jira-work-get.json)
- [`jira-work-state.json`](../examples/jira-work-state.json)

The examples are structural templates, not ready-made authority. A project
route must supply its exact active capability-binding ID when creating an
operation. A successful `work.get` remains an explicit read: import it with
`work-item import-operation`; neither list nor get silently synchronizes MGB.

## Effect and recovery semantics

`work.state` v1 currently accepts exactly `{"desired_state":"done"}` for one
issue key inside the mapped project. It first reads the issue's status:

- if the private target status is already present, it succeeds without a
  transition;
- otherwise it discovers exactly one available transition to that status,
  applies it, and reads the status again before claiming success;
- if no unique transition exists, it returns a conflict without dispatching an
  effect; and
- if the transition reply or post-effect observation is lost, it returns
  `uncertain`.

Reconciliation performs only the exact status read. It reports success when
`done` is observed and `not_found` otherwise; it never repeats the transition.
A later distinct operation is also idempotent because its first observation
recognizes the already-completed state. The successful result is the published
provider-neutral
[`work-item-state-result-v1`](../schemas/work-item-state-result-v1.schema.json)
shape, not a Jira status or transition document.

The HTTPS carrier rejects redirects, non-HTTPS base URLs, foreign hosts,
unrecognized endpoints, unbounded reads, malformed JSON, and invalid retry
headers. It honors Jira `Retry-After` throttling and never copies an upstream
error body into a result or diagnostic. See Atlassian's
[rate-limit guidance](https://developer.atlassian.com/cloud/jira/platform/rate-limiting/).

## Deliberate limits

- This provider targets Jira Cloud REST v3; it does not claim Jira Data Center
  compatibility.
- `work.list` is a bounded newest-updated discovery window, not a mirror or
  synchronization cursor.
- Only the initial core-owned `done` state is supported. Reopening, assignment,
  comments, arbitrary fields, and arbitrary transitions are not capabilities.
- No Jira credential, base-URL configuration path, JQL, provider status ID,
  transition ID, raw body, or socket path is worker-visible.
- The deterministic M14.4 proof imports Jira work and carries it through the
  complete GitHub-backed producer/reviewer/maintainer and landing path. The
  remaining live Jira call is intentionally gated on an explicitly selected
  private credential file, mapped project, and read-safe issue key; MGB does
  not discover those from the workstation.
