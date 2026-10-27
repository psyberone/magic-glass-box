# GitHub reference provider

**Status: completed 2026-10-21.**

Milestone 11 supplies a separate `magic-glass-box-github-provider` process. It
uses the operator's explicitly selected `GH_CONFIG_DIR`, exposes only the
service-sidecar protocol over a private Unix socket, and returns bounded
normalized evidence. The reusable GitHub credential is never copied into a
worker, an MGB operation document, SQLite, or a log.

The reference provider implements eight v1 capabilities:

| Capability | Mode | Enforced contract |
| --- | --- | --- |
| `work.get` | read | Return one exact issue as the existing import-ready work-item v1 envelope |
| `work.list` | read | Return one bounded page of normalized issue metadata, excluding pull requests |
| `change.get` | read | Return normalized metadata for one exact pull request |
| `change.list` | read | Return one bounded page of normalized pull-request metadata |
| `refs.publish` | effect | Create one previously absent branch at one exact candidate commit |
| `change.open` | effect | Find or open one exact head/base proposal without duplicating it |
| `checks.read` | read | Read named latest check-runs or legacy status contexts plus latest reviewer decisions for one exact head |
| `landing.invoke` | effect | Merge only the authorized pull-request head, then report the actual base ref and closed-reference set |

`landing.invoke` truthfully reports `head_enforced`. GitHub's REST
[merge endpoint](https://docs.github.com/en/rest/pulls/pulls#merge-a-pull-request)
accepts the expected pull-request head but does not atomically compare the
expected base revision. MGB therefore never upgrades it to
`base_and_head_enforced`. Base drift is discovered from the provider's actual
post-operation ref and fed back into the integration/refold ledger. The carrier
pins GitHub's supported REST API version
[`2026-03-10`](https://docs.github.com/en/rest/about-the-rest-api/api-versions).

## Credential identity

Inspect the chosen GitHub CLI profile before registering authority:

```console
magic-glass-box-github-provider inspect \
  --gh-config-dir /absolute/private/gh-config \
  --host github.com
```

The command calls `gh` with a minimal environment and emits only the account,
host, and a one-way credential-scope fingerprint. It does not print the token.
Copy those non-secret values into
[`examples/github-service-sidecar.json`](../examples/github-service-sidecar.json),
along with the repository, socket path, and a deployment digest identifying the
provider build being authorized.

Register the sidecar and bind only the capabilities the project should have:

```console
magic-glass-box service-sidecar register github-main \
  --spec /absolute/private/github-service-sidecar.json \
  --created-by operator
magic-glass-box capability bind demo-project work.get --version 1 \
  --sidecar github-main --mapping primary --bound-by operator
magic-glass-box capability bind demo-project work.list --version 1 \
  --sidecar github-main --mapping primary --bound-by operator
magic-glass-box capability bind demo-project change.get --version 1 \
  --sidecar github-main --mapping primary --bound-by operator
magic-glass-box capability bind demo-project change.list --version 1 \
  --sidecar github-main --mapping primary --bound-by operator
magic-glass-box capability bind demo-project refs.publish --version 1 \
  --sidecar github-main --mapping primary --bound-by operator
magic-glass-box capability bind demo-project change.open --version 1 \
  --sidecar github-main --mapping primary --bound-by operator
magic-glass-box capability bind demo-project checks.read --version 1 \
  --sidecar github-main --mapping primary --bound-by operator
magic-glass-box capability bind demo-project landing.invoke --version 1 \
  --sidecar github-main --mapping primary --bound-by operator
```

No binding means no project-routed operation. There is no discovered default
account, repository, provider, or fallback client.

## Running the provider

```console
magic-glass-box-github-provider serve \
  --gh-config-dir /absolute/private/gh-config \
  --spec /absolute/private/github-service-sidecar.json \
  --socket-path /absolute/private/github.sock \
  --candidate-root /absolute/private/mgb-state/candidates \
  --max-invocations 100
```

`--candidate-root` is required when the registration advertises
`refs.publish`. The provider accepts a source repository only beneath that
root, resolves the immutable `refs/mgb/candidates/<sha>` ref, verifies it equals
the authorized SHA-1 commit, and pushes that SHA to an explicitly named new
branch. When the provider is separately containerized, mount this root
read-only. The provider constructs the Git and GitHub commands itself; neither
the agent nor an operation document supplies argv, a remote URL, a credential
helper, or a token.

The reference operation templates are:

- [`github-work-get.json`](../examples/github-work-get.json)
- [`github-work-list.json`](../examples/github-work-list.json)
- [`github-change-get.json`](../examples/github-change-get.json)
- [`github-change-list.json`](../examples/github-change-list.json)
- [`github-ref-publication.json`](../examples/github-ref-publication.json)
- [`github-change-open.json`](../examples/github-change-open.json)
- [`github-checks-read.json`](../examples/github-checks-read.json)

They are structural examples, not ready-made authority: replace every mapping,
revision, ID, branch, candidate-store path, and idempotency key with values from
the exact project/candidate decision. Project-routed creation must pass the
corresponding `cpb_...` binding ID so the operation and route are pinned in the
same transaction.

The legacy GitHub list operation accepts an explicit page cursor from 1 through
10,000, a limit from 1 through 100, and `open`, `closed`, or `all` state. Those
GitHub-owned selection values remain operation input; the normalized
`work.list` receipt exposes only bounded references and whether the selected
window was truncated. It never contains a provider cursor, status name, issue
body, comment, transcript, provider URL, or raw response. `work.get`
returns exactly one normalized work-item v1 document, including the bounded
issue body as its execution brief (or the title when the body is blank). That
document is still untrusted content and is not imported automatically: an
operator deliberately passes it through the ordinary work-item import command.
`change.get` and `change.list` similarly return bounded change metadata, not a
pull-request transcript or patch.

## Landing from the integration queue

After gates and dependencies are ready, claim the serialized lane and issue a
provider landing for the already-open pull request:

```console
magic-glass-box integration claim iqm_<id> \
  --owner supervisor --expected-version <version>
magic-glass-box integration provider-issue iqm_<id> \
  --owner supervisor --expected-version <claimed-version> \
  --resource-id <pull-number> --merge-method squash --issued-by operator
magic-glass-box integration provider-execute plr_<id> \
  --owner supervisor --settled-by operator
```

Issuance atomically consumes the claim, rechecks exact candidate/gate/base
authority, resolves the active `landing.invoke` binding, creates the service
operation, and pins both records. Execution uses the ordinary durable
invoke/reconcile state machine. `provider-show` exposes the landing and its
operation together; the control room projects the pending provider landing,
operation state, and attention item.

If the merge or push response is lost, the operation becomes `uncertain`.
Reconciliation reads the deterministic pull request or ref and never repeats
the effect. A confirmed non-landing returns the queue to `queued` when the base
is unchanged, or advances the observed baseline and requires refolds when it
drifted. A confirmed landing advances MGB to GitHub's actual observed base
revision, which may be a merge or squash commit rather than the candidate head.

## Live exit proof

The destructive proof harness requires an exact clean clone, base revision,
repository, account, branch namespace, and explicit acknowledgment:

```console
PYTHONPATH=src python3.11 scripts/check_live_github_provider.py \
  --gh-config-dir /absolute/private/gh-config \
  --repository owner/sacrificial-repository \
  --source-repository /absolute/clean/sacrificial-clone \
  --expected-base-revision <40-character-sha1> \
  --base-ref refs/heads/main \
  --proof-id <unique-lowercase-id> \
  --expected-account <github-login> \
  --i-understand-this-publishes-a-branch-opens-and-merges-a-pr
```

It constructs a deterministic child commit in a temporary bare candidate
store without modifying the source checkout. It then exercises `work.list`,
`refs.publish`, `change.open`, `change.get`, `change.list`, `checks.read`, and
`landing.invoke` through the Unix sidecar. The landing request is sent and its
response deliberately discarded; the same operation is reconciled by reading
the exact pull request and base ref. Output is a bounded report containing no
raw provider response, issue/PR title, token, or configuration path.

The accepted M11 run targeted `psyberone/imitation-krab` at base
`50fcd1e55e8034e8ea3a931de0714e710e912b06`. It published candidate
`f5c8d1fe58229a8e08515a0e07fba187a1758b33`, opened and squash-merged PR 1,
discarded the successful landing response, and reconciled the provider-observed
base `83eaec71d6047e06f6a249852159c897871fc680` without a second merge. The first
attempt exposed a real wire-contract defect: `change.get` put a head revision
on a metadata-only target. A RED regression test now requires the revision to
remain in the normalized result while the generic receipt revision stays null.

The post-live regression checkpoint passes 982 tests with 86% aggregate
statement-plus-branch coverage and 90% coverage for the provider module. Ruff
format and lint are clean. A clean current-provider mutation pass killed 2,272
of 2,717 mutants (83.62%) with no timeouts, unchecked mutants, crashes, or
suspicious outcomes. Mutation-driven assertions now bind the exact-read HTTP
status and require GitHub's `merged` field; the only surviving `change.get`
mutants alter its human-readable diagnostic label.

The proof intentionally leaves the merged pull request and published branch as
audit artifacts. Cleanup is operator-owned because branch deletion and pull
request closure are not capabilities exposed by this provider. The harness
also refuses to target `psyberone/magic-glass-box`.

## Deliberate limits

- The provider does not expose raw `gh`, raw `git`, arbitrary REST endpoints,
  arbitrary remotes, or a network socket to workers.
- It does not synchronize issue bodies, PR transcripts, or comments. Exact
  issue reads normalize a bounded body into a work-item brief only when the
  corresponding read operation is explicitly invoked.
- GitHub repository publication supports SHA-1 candidates because GitHub's Git
  object IDs are SHA-1 in this v1 contract.
- The local deterministic, real-Unix, and explicitly authorized live-repository
  proofs are implemented.
