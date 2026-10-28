# Capability providers and project routing

**Status: implemented foundation; M14 mixed-provider expansion and M15
controlled remote-execution proofs complete.**

Schema v24 implements the first narrow routing seam: core-owned capability
modes, explicit single-controller project bindings, audited unbinding, and an
atomic operation-to-binding pin. Schema v42 adds the first heterogeneous
resource authority without changing that routing model. Profile/run service
grants remain later work; their absence does not widen the implemented surface.

Magic Glass Box exposes small, versioned semantic capabilities rather than
provider-native clients. An agent asks for a kind of work to be performed; it
does not select Jira, GitHub, an SSH host, a model endpoint, a credential, or a
transport. Project policy routes that capability to an explicitly configured
provider, and the provider keeps its credentials and implementation details
outside the worker and the MGB core.

This is a ports-and-adapters model with an out-of-process security boundary.
“Capability provider” names the architectural extension. “Sidecar” describes
one way that extension is deployed and isolated.

## Vocabulary

| Term | Meaning |
| --- | --- |
| Capability surface | A core-owned semantic operation such as `work.get`, `checks.read`, or `landing.execute` |
| Provider | An implementation of one or more capability versions, such as GitHub, Jira, or a controlled runner |
| Controller | One configured provider instance with a pinned deployment, resource, and credential scope |
| Project binding | An explicit route from one capability to one authorized controller |
| Sidecar | An out-of-process provider deployment that owns provider code, credentials, and network access |
| Runtime adapter | A local worker-process contract; it is not a service provider merely because the worker is an agent |

Agents address capability surfaces through MGB. They never address a provider
transport directly:

```text
networkless agent
       |
       | semantic request or proposal
       v
Magic Glass Box -- policy, authority, ledger, limits --> project binding
                                                            |
                                                            v
                                                  capability provider
                                                            |
                                                            v
                                                   external service
```

MGB remains the authority for candidate readiness, work lifecycle, budgets,
dependency order, and landing policy. A provider translates an already
authorized semantic operation into provider mechanics and returns a bounded,
normalized result. It does not gain the MGB database, worker workspace, or a
general callback into the control plane.

## Effective capability surface

A capability is available to a run only through the intersection of all
applicable authorities:

```text
effective capabilities
  = core-defined operations
  ∩ provider-supported versions
  ∩ explicit project bindings
  ∩ profile/run grants
  ∩ live, unexpired, unrevoked authority
```

This produces deliberate negative space. If a project has no provider bound to
an operation, that operation is disabled: it is not advertised to the agent
and a direct attempt fails closed with a stable unavailable classification.
MGB does not discover a provider, select a default account, fall back to a raw
CLI, or route to another integration implicitly.

Granularity is semantic and bounded rather than mirroring every vendor HTTP
endpoint. A project may, for example, permit `work.get` while leaving
`work.comment` and `work.transition` unbound. Read, proposal, and effect
authority remain distinct. Agent proposals do not become supervisor authority
merely because the same provider could perform the effect.

Effect operations have one authoritative controller for a given project and
resource. Read or evidence operations may have multiple explicitly named
sources only when the core contract defines how their evidence remains
separate or is combined. There is no implicit provider failover: changing the
controller, account, or credential scope is an audited authority change.

Unbinding a provider disables new requests without erasing its history.
Already-dispatched or uncertain effects retain their exact provider identity so
they can be settled or reconciled under explicit operator policy. A hard
provider revocation remains a kill switch and may intentionally leave an
external outcome unresolved rather than silently reacquiring authority.

## Mixed-provider project example

One project can compose providers without making an agent understand the
composition:

| Capability | Bound provider |
| --- | --- |
| Work intake and status | Jira project `ABC` |
| Change proposals and pull requests | GitHub repository `acme/widgets` |
| Hosted checks | GitHub Actions for `acme/widgets` |
| Heavy validation | Operator-defined Azure runner profile |
| Landing | GitHub repository `acme/widgets` |
| Human interaction | Imitation Krab project binding |
| Inference | Exact grants selected by the agent profile |

The agent sees normalized work, change, validation, interaction, and inference
surfaces. Jira issue keys, GitHub API shapes, SSH arguments, remote URLs, and
provider authentication remain controller details. Cross-provider correlation
uses MGB's durable project, work-item, candidate, operation, and evidence IDs;
provider object IDs are bounded external references, not authority conveyed in
message text.

## Extension classes

The umbrella architecture contains several deliberately different contracts:

- **service providers** perform durable reads and effects such as work,
  changes, checks, refs, and landing;
- **inference connectors** perform transient, quota-consuming model calls with
  separate budget and ambiguity rules;
- **runtime adapters** launch local agent processes in MGB-controlled workers;
- **execution providers** submit, inspect, cancel, reconcile, and collect
  bounded evidence from remote jobs through the M15 controlled-runner
  contract; and
- **interaction providers** may later carry correlated human questions,
  approvals, notifications, and responses.

These classes may reuse framing, registration, deadline, credential-isolation,
and audit primitives. They must not be collapsed into an arbitrary remote-call
plugin. A controlled SSH runner, for example, would expose a pinned test recipe
against an exact candidate—not a hostname and shell command chosen by an
agent.

## Provider ecosystem rules

- Provider code remains out of process; MGB does not import third-party plugin
  code into the trusted core.
- The core owns capability names, versions, schemas, authorization points, and
  lifecycle meaning.
- A provider declares only the exact capability versions it implements, and
  the operator configures controller instances from that declared surface.
- Registrations pin deployment identity, resource mappings, credential-scope
  identity, transport, rate, concurrency, duration, and byte limits without
  containing the credential.
- Inputs and outputs are strict, bounded, and provider-neutral. Raw upstream
  payloads, unrestricted message bodies, and reusable credentials do not cross
  the boundary.
- Each capability family requires a conformance suite. Passing transport tests
  alone does not establish correct semantic behavior.
- Vendor-specific experimental operations may remain operator-only, but they
  do not become agent-facing capabilities until the core defines their
  authority and durable meaning.

This is an extension ecosystem, not necessarily a public marketplace. MGB can
start with operator-owned providers and add contracts only when a real
integration supplies enough evidence to define the common semantic surface.

## M10 and M11 scope stability

Milestone 10 remains the credential-free operation kernel: immutable
registration, exact operation authority, bounded local transport, durable
attempts, typed outcomes, and uncertain-effect reconciliation. Its v1 service
contract deliberately begins with `work.*`, `change.*`, `checks.*`, `refs.*`,
and `landing.*` plus repository-shaped mappings.

The bounded M10.1 stabilization pass implemented only the M11-facing seams: the
core owns bindable capability modes; projects explicitly bind exact capability
versions to one sidecar mapping; project-routed operations atomically pin that
binding; and ambiguous or expired reads become retryable while effects retain
the reconciliation path. Provider naming, v1 repository-shaped payloads, Unix
transport, and operator-only extension registration remain unchanged.

Milestone 11 implemented the GitHub reference provider and integration-queue
binding. The implementation covers exact candidate-ref
publication, bounded normalized work/change reads, deterministic change
creation, named checks and approvals, truthful landing assurance, lost-response
reconciliation, and credential containment. Its live repository exit proof
published, opened, landed, and reconciled an exact disposable candidate without
duplicating the merge. It did not add Jira, GitLab, remote runners, dynamic
plugin loading, a provider marketplace, or a universal resource schema.

The capability-provider direction does not retroactively change M10 or M11.
M14 advances the service contract explicitly: v1 stays repository-shaped and
digest-stable, while v2 replaces the repository field with a core-owned
`resource` tuple. The initial kinds are `repository` and `work_project`;
provider-native labels such as `jira_project` do not enter core authority.
Adding another kind requires a core contract change, not a sidecar invention.

## Post-M11 sequence

M12 now proves that the producer/reviewer/maintainer sequence can consume a
provider through a durable bounded workflow, including controller deployment
rotation and effect-receipt recovery. Provider breadth remains deliberately
sequenced behind demonstrated needs, not blocked by the architecture: workflow
templates consume semantic operations and never name GitHub, Jira, SSH, or a
model provider.

The provider pressure tests are intentionally heterogeneous:

1. the implemented and live-proven Jira work provider paired with GitHub
   candidates and landing in one M14.4 project;
2. the implemented controlled execution provider for an exact candidate and
   pinned remote test recipe, with no agent-selected SSH target or command;
   its live remote-host exit proof completed M15, while the planned
   [containerized runner appliance](containerized-runner-appliance.md) changes
   deployment rather than authority; and
3. a second forge provider, with the owned GitLab.com mirror available as the
   first conformance target, driven through the same contract rather than
   copied GitHub mechanics.

The owned Bitbucket Cloud mirror supplies another live forge target. Azure
DevOps, raw Git, additional issue systems, interaction carriers, local and
hosted agents, and additional inference providers remain candidates selected
by demonstrated need. See [roadmap.md](roadmap.md) for the milestone boundaries.

M14's deterministic mixed-provider proof exercises the first item end to end:
Jira v2 work intake is explicitly imported with immutable operation
provenance, while exact candidate publication, change creation, checks, and
landing route through four GitHub v1 bindings. The workflow and workers retain
only MGB-owned correlation and normalized work data. Removing either provider
surface fails closed without discovering or falling back to the other.

The separately authorized live proof confirmed the same external boundaries:
one read-only Jira Cloud issue became a provenance-linked MGB work item, while
an exact disposable GitHub candidate was published, opened, checked,
squash-landed, and reconciled after an intentionally lost response. Neither
reusable credential entered MGB state or a worker-visible projection.
