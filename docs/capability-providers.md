# Capability providers and project routing

**Status: architectural direction; this does not expand the Milestone 10 or
Milestone 11 MVP exit proofs.**

Schema v24 implements the first narrow routing seam: core-owned capability
modes, explicit single-controller project bindings, audited unbinding, and an
atomic operation-to-binding pin. Profile/run grants and heterogeneous resource
bindings remain later work; their absence does not widen the implemented
surface.

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
- **execution providers** may later submit, inspect, cancel, reconcile, and
  collect bounded evidence from remote jobs; and
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

The bounded M10.1 stabilization pass implements only the M11-facing seams: the
core owns bindable capability modes; projects explicitly bind exact capability
versions to one sidecar mapping; project-routed operations atomically pin that
binding; and ambiguous or expired reads become retryable while effects retain
the reconciliation path. Provider naming, v1 repository-shaped payloads, Unix
transport, and operator-only extension registration remain unchanged.

Milestone 11 remains the GitHub reference provider and integration-queue
binding. It proves exact authorized GitHub operations, truthful assurance,
lost-response reconciliation, and credential containment. It does not add
Jira, GitLab, remote runners, dynamic plugin loading, a provider marketplace,
or a universal resource schema.

The capability-provider direction may change how the GitHub implementation is
factored and named, but not what either milestone must prove. A later
heterogeneous implementation—such as Jira or a controlled Azure runner—should
pressure-test a versioned resource-binding model rather than pretending every
external resource is a repository. Existing v1 contracts should be preserved
or advanced explicitly; provider-specific fields must not leak into the agent
surface to avoid a version change.
