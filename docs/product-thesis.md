# Product thesis

## Position

**Disposable agents. Durable work.**

Magic Glass Box is a local-first control plane for trusted work performed by
untrusted agents. It does not try to make an agent smarter or prescribe its
reasoning loop. It makes delegated work survivable, inspectable, and bounded
when the worker is headless, replaceable, or abruptly gone.

The primary audience is an individual developer or small team that wants to
operate one or more coding agents with production-grade discipline without
ceding source code, credentials, or operational state to an agent vendor.

## Product contract

Magic Glass Box should preserve five guarantees:

1. **Work outlives workers.** Assignments, pinned context, pending questions,
   decisions, and outcomes remain durable when an agent or container exits.
2. **Authority is explicit.** Each run receives only its declared source
   snapshot, tools, gateway operations, identity, and named authorities.
   Reusable credentials stay on the trusted side of the gateway. Authority is
   scoped, revocable, and never inferred from message text.
3. **Human interaction is asynchronous.** A question belongs to the work item,
   not a terminal prompt. A person can answer later and a different compatible
   worker can continue without an operator acting as a cron job.
4. **Visibility does not require surveillance.** Operators can see ownership,
   state, blockers, grants, and results without Magic Glass Box retaining
   private message bodies or agent transcripts.
5. **Agent runtimes are replaceable.** Profiles and adapters may target Codex,
   Claude Code, OpenHands, or a custom headless process. Durable semantics do
   not depend on one model or vendor session.

## Ownership boundary

Magic Glass Box owns execution policy, immutable run inputs, lifecycle state,
interaction correlation, credential grants, cleanup, and audit metadata.

External work enters through a versioned, forge-neutral import document. Its
text is untrusted input, not authority: it cannot choose a project, profile,
credential, network policy, or lifecycle transition. Magic Glass Box does not
silently synchronize work items back to their source.

Magic Glass Box owns integration semantics: candidate revisions, dependencies,
exact-head gates, authorization, and durable receipts. It does not bake a forge
client or forge credential into the core. Optional provider sidecars may perform
explicitly authorized effects using user-supplied configuration that is mounted
only into that sidecar. Agents receive narrow capabilities, never the
configuration directory or reusable credential.

Imitation Krab owns identities, project roles, private message content,
sessions, work claims, delivery, and message status. Magic Glass Box may retain
stable Krab object IDs and trusted lifecycle metadata, but not duplicate bodies
or transcripts.

The worker owns only its current attempt. It is not the authority for durable
state and cannot grant itself additional access.

## Golden-path proof

The product thesis is proven when one local demonstration can:

1. assign a run to one agent runtime with a pinned repository snapshot;
2. authorize one named service through the gateway while denying undeclared
   credentials and direct network access;
3. turn a structured agent question into a durable pending interaction and a
   private Krab delivery;
4. terminate the original worker without losing the interaction;
5. accept a human response later;
6. automatically advance the answered work with a different compatible worker;
7. collect independently authored results from parallel profile-pinned
   assignments after replacing the original coordinator; and
8. escalate a deliberately stalled interaction once at each pinned threshold;
   and
9. show the exact context, authority, transitions, and outcome without showing
   either private message body.

## Non-goals

Magic Glass Box is not:

- a general chat service or replacement for Imitation Krab;
- an agent reasoning framework, prompt framework, or model router;
- a general-purpose DAG or business-workflow engine;
- a built-in GitHub or GitLab client or credential store;
- a transcript warehouse or chain-of-thought viewer; or
- a promise that sandboxing makes arbitrary code safe on every host.

## Feature test

A proposed feature belongs in the core when it materially improves at least
one of these questions:

- What durable work exists, who owns it, and what is blocking it?
- What exact authority did a worker receive?
- Can the worker be stopped, replaced, or recovered safely?
- Can a human participate without sharing a terminal session?
- Can an operator reconstruct system decisions from bounded metadata?

Features that primarily make one model reason better belong in an agent runtime
or adapter, not in the control plane.
