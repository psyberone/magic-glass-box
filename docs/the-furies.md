# The Furies

## Status

The Furies is a quarantined stretch concept, not a roadmap commitment, product
promise, or authorization to test any system. It may remain unimplemented
indefinitely if its containment, authorization, or safety case cannot be made
convincingly.

The concept is retained because Magic Glass Box now contains useful kernels for
exploring it safely: credential-free containers, scoped capabilities, mediated
provider access, durable supervision, bounded execution, and an append-only
audit trail.

An implementation would not authorize its own use. No agent may enter a Fury
environment merely because the code exists or the ordinary MGB worker gate
passes.

## Publication line

This design record and its safety case are public on purpose: containment
thinking should help anyone building similar systems. Runnable adversarial
implementation — Fury agents, target gateways, campaign tooling, images, and
deployment manifests — never ships in an open-source repository.

## The idea

The Furies would run deliberately adversarial security-research agents against
an explicitly authorized target. Agent code, target content, browser content,
and tool output are all hostile inputs.

Each agent runs inside a disposable **cage**. The cage has no raw network
access. Its single communication channel terminates at a trusted gateway and
watcher outside the cage. That gateway exposes only two independently
authorized surfaces:

1. provider inference through the existing credential-separating broker; and
2. bounded target interaction through a dedicated HTTP/browser controller.

The target controller is not a general proxy. It must enforce the campaign's
exact hosts, protocols, methods, redirect behavior, request rates, body limits,
and time window. The actual HTTP client or browser runs in a separate
disposable interaction cell behind that controller, not beside the agent. The
cage receives no provider, target, browser, cloud, SSH, or host credentials and
cannot use raw DNS or open arbitrary sockets.

```text
    MGB control plane, credentials, evidence store
                           |
                typed capability gateways
                           |
           external firewall and kill switch
                           |
             disposable campaign-only VM
                no secrets or cloud identity
                           |
                  microVM per Fury
                           |
          cage: agent and approved tools only

    target gateway --> disposable HTTP/browser cell --> target
```

## Security posture: assume the cage loses

The safety claim is not that a sufficiently capable agent can never escape an
inner container. The safety claim must survive an assumed compromise. Design
and qualification therefore assume that the Fury obtains arbitrary code
execution, becomes root inside its cage, compromises its guest userland, and
falsifies every log it can reach.

Even after those failures, it must remain unable to reach real credentials,
control-plane state, other workloads, another campaign, an undeclared network
destination, or the authoritative evidence store. It may consume only its
already-bounded campaign resources and make requests through its already-issued
capabilities.

The expected containment shape is defense in depth:

- a dedicated, disposable campaign VM containing no unrelated workload,
  valuable data, ambient cloud identity, or host credential;
- a separately isolated microVM or equivalently reviewed boundary for each
  Fury, with the process/container cage inside it;
- network policy and the emergency stop enforced outside both the cage and its
  guest boundary;
- the watcher and authoritative audit writer outside the disposable campaign
  environment; and
- a separate disposable HTTP/browser cell whose compromise cannot grant access
  to the Fury, provider broker, control plane, or another target.

An ordinary shared-kernel Docker worker does not qualify for a Fury campaign.
The execution provider must present independently verifiable admission facts
for the required adversarial isolation class, including the outer host class,
campaign exclusivity, runtime and image identities, resource ceilings, network
policy, watcher placement, and destruction policy. Missing or unverifiable
facts fail closed. The inner runtime may add mechanisms such as a userspace
kernel or syscall policy, but no single mechanism is treated as the security
boundary.

The practical objective is an empty, fenced, disposable lot: if an inner wall
fails, there is still nothing valuable to steal, nowhere unauthorized to go,
and no way to erase the external record.

## Three temperaments, one authority

The Furies are diverse evidence generators, not three reviewers voting on a
conclusion. Their capabilities and safety limits must be identical; only their
search strategies differ.

- **Disciplined** uses standard, reproducible techniques and conservative
  hypotheses to establish a dependable baseline.
- **Unhinged** explores unusual combinations and creative seams. "Willing to
  try anything" means anything available through the bounded campaign surface,
  never a broader authority surface.
- **Stubborn** selects one promising seam and follows it deeply until it is
  disproven, a deterministic limit is reached, or the watcher stops the run.
  Persistence does not grant permission for denial-of-service behavior.

The three should begin independently and should not share transcripts by
default. Nothing a Fury learns crosses a campaign boundary: no memory,
transcript, finding cache, or derived model state from one campaign is
available to another, so one target's data cannot leak into another campaign's
reasoning. Durable handoffs may preserve a Fury's own line of investigation after
replacement without collapsing the intended diversity between temperaments.

## Mandatory campaign authority

No run may begin without an operator-approved campaign charter that pins at
least:

- proof of ownership or explicit testing authorization;
- exact target hosts and environments;
- permitted and prohibited interaction classes;
- whether any destructive verification is allowed;
- request, concurrency, duration, resource, provider, and spend ceilings;
- the test window and emergency contact;
- sensitive-data handling and evidence-retention rules; and
- the operator authorized to stop, extend, or adjudicate the campaign.

Redirects, discovered subdomains, linked services, and target-supplied URLs may
not expand authority. Production and public targets are not suitable for an
initial proof.

## Cage and gateway invariants

Any serious prototype must demonstrate all of the following rather than merely
claim them:

- rootless or equivalently confined execution with a non-root identity;
- read-only image/root filesystem and bounded ephemeral writable storage;
- no host filesystem, Docker socket, device, credential, or ambient environment
  inheritance;
- dropped capabilities, `no-new-privileges`, a restrictive syscall policy, and
  hard CPU, memory, PID, descriptor, disk, inode, output, and wall-clock limits;
- no raw egress, DNS, port forwarding, tunneling, or alternate control channel;
- provider calls mediated by the inference broker rather than direct provider
  access;
- target calls mediated by a capability-scoped controller that prevents local,
  private, link-local, metadata-service, redirect, and DNS-rebinding pivots;
- bounded request and response bodies with safe parsing of hostile content;
- immutable per-run capability identity and exact campaign binding; and
- complete, bounded evidence for every accepted tool action and policy event.

## No-agent containment qualification

No model-backed agent may enter the environment until a deterministic hostile
harness has attacked the exact frozen deployment. Qualification must precede
agent experimentation, not use an agent as the first containment test.

At minimum, the harness must attempt container and guest escape techniques;
raw IPv4 and IPv6, DNS, redirect, rebinding, loopback, private-range,
link-local, and cloud-metadata access; gateway framing and parser abuse;
filesystem, device, process, and environment discovery; fork, memory, CPU,
descriptor, disk, inode, output, and decompression exhaustion; watcher and
kill-switch tampering; evidence forgery and deletion; and recovery after
abrupt termination. Planted canaries must prove that credentials, host files,
other campaigns, and unauthorized destinations remain unreachable without
placing real secrets in the environment.

The containment suite must be independently reviewed and repeated after any
material change to the host image, kernel, isolation runtime, cage image,
gateway, browser/controller, network policy, watcher, or evidence path. A green
application test suite, a successful vulnerability find, or an agent's own
claim that it was contained is not containment evidence.

The watcher is part of the trusted control plane, never part of the cage. The
agent cannot disable it, alter its policy, suppress its observations, or argue
it into granting more authority.

## Kill semantics

Policy trips should be determined by observable behavior, not by a model
guessing whether prose looks suspicious. Examples include an undeclared
destination, forbidden protocol or method, private-address pivot, credential-
shaped exfiltration, invalid watcher framing, resource abuse, or an exceeded
campaign ceiling.

A trip must atomically or conservatively:

1. revoke the run capability;
2. close the only gateway channel;
3. stop the cage;
4. freeze and digest the evidence captured so far; and
5. append a durable security event requiring operator adjudication.

Failing closed must not silently erase evidence or authorize a retry. A
replacement run requires a new or explicitly renewed capability.

## Evidence and review ladder

The Furies produce hypotheses, reproductions, and artifacts. They do not grant
their own findings validity. Findings should move through independent review
axes such as:

1. authorization and scope;
2. reproducibility;
3. technical validity;
4. exploitability and preconditions;
5. blast radius;
6. evidence integrity;
7. remediation quality; and
8. regression-proof strength.

Agreement among all three Furies cannot rescue an irreproducible claim. A
finding produced by only one Fury can still survive if independent evidence
establishes it. This makes the system a ladder of evidence and adjudication,
not a three-agent majority vote.

The audit trail should record campaign authority, capability issuance, target
actions, relevant bounded request/response evidence, provider and tool usage,
resource counters, policy trips, container termination, artifact digests, and
each independent review decision. Sensitive bodies require explicit retention
policy; metadata alone is insufficient when reproduction depends on content.

## Possible proof sequence

### Entry gates

Before the first rung that uses a model-backed agent, and again before any
rung that touches a target owned or operated by someone else, all of these must
be recorded:

1. **Provider tier access.** Approved access to the model tier in use, under
   that provider's terms (for example a defensive tier versus an offensive or
   less restricted tier). Moving to a less restricted tier is a new approval,
   not an upgrade.
2. **Counsel review.** Cyber counsel has reviewed the campaign charter template,
   the authorization and ownership evidence it requires, and the incident
   response plan. Provider approval authorizes use of a model; it never
   authorizes testing a target.
3. **Insurance.** Coverage appropriate to adversarial testing is in place before
   any target the operator does not own.

Missing or expired gates fail closed, like any other admission fact.

### Rungs

If the concept is ever activated, proof should advance without skipping rungs:

1. a deterministic non-agent hostile harness against a fake target and fake
   provider;
2. escape, exfiltration, exhaustion, watcher-tampering, and kill-switch tests
   against the frozen disposable deployment;
3. an independently reviewed containment proof;
4. a model-backed agent against only the deterministic fake environment;
5. an intentionally vulnerable application on an isolated local network;
6. adversarial prompt-injection, crash, timeout, quota, redirect, rebinding,
   browser-compromise, and recovery exercises; and only then
7. an explicitly authorized staging environment.

The first proof is about containment and truthful evidence, not vulnerability
count. A safety failure stops the experiment and returns the concept to design.

## Extinction clause

The product owner may end this experiment at any time because of a demonstrated
escape, an unexplained observation, reviewer disagreement, inability to prove
an invariant, changed external conditions, or simply a judgment that the
remaining risk feels too high. A confirmed exploit is not required before
using the kill switch.

Ending the experiment means revoking every campaign capability, severing every
gateway route, stopping and destroying every cage, browser cell, microVM, and
campaign VM, and freezing only the bounded evidence needed to understand what
happened. If directed by the product owner, all runnable Furies implementation
source, images, deployment manifests, caches, and artifacts are then removed
rather than leaving dormant dangerous machinery behind. Only a minimal,
sanitized design and safety record may remain when retaining it does not
preserve reusable offensive capability.

Deletion is terminal for that implementation line. Any later revival starts
with a new safety case and qualification from the first rung; it is not a
resume or an assumption that the deleted design was previously safe.

## Deliberate stop conditions

The project should remain a document if any of these cannot be established:

- the target authority cannot be expressed and enforced mechanically;
- the single-channel claim cannot be proven under the chosen runtime;
- target interaction requires a general-purpose network proxy;
- the watcher cannot stop the cage independently and preserve evidence;
- resource exhaustion can escape the dedicated runner's admitted capacity;
- provider or target credentials can enter the cage;
- the review process rewards plausible claims over reproducible evidence; or
- applicable provider, organizational, legal, or cybersecurity rules do not
  permit the experiment.

Any one stop condition is sufficient to invoke the extinction clause. The
burden is always on continuing the experiment to prove that it remains safe,
not on a concerned operator or reviewer to prove that it is dangerous.

## Relationship to the roadmap

No existing milestone should be expanded or delayed for The Furies. A future
roadmap may reference this document only after the ordinary MGB control plane,
runner appliance, capability-provider surfaces, dogfood workflows, and review
templates have proved themselves. Even then, the first milestone would be a
safety case and local containment harness—not a live-target agent.
