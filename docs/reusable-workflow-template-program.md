# Practice-informed reusable workflow templates

**Status: Milestone 17 consultation in progress; foundation, review shape, and
lifecycle resolved, with boundary and closeout under consultation; Milestone
18 remains the first delivery.**

MGB already implements fixed, versioned `discovery-v1`, `implementation-v1`,
and `advisory-review-v1` prescriptions. The next template work is not a request
for a general workflow builder. It is a bounded effort to determine which
parts of repeatedly successful coordination practice should become reusable
product contracts, which should remain project configuration, and which are
Panopticon-specific and must stay outside MGB.

The Panopticon coordinator has already supplied a durable practice memo drawn
from running a large producer/reviewer/landing operation by hand. That memo is
an evidence source, not an automatic specification. MGB's completed dogfood and
coordinator-reconciled postmortem now provide the second evidence source.

Three retained M16 rounds exposed and remediated a deterministic no-change
retry and a review-verification deadlock, then completed exact-candidate review,
immutable validation, maintainer approval, provider landing, and receipt-gated
closeout. The debrief also retained two post-approval harness failures, manual
dead-driver detection, one provider-native metadata correction, and the limits
of a one-file proof. These observations are inputs to consultation, not an
implicit template design.

## Engagement sequence

| Point | Coordinator role | Deliberate limit |
| --- | --- | --- |
| Post-M15 audit | Operate or advise the Panopticon review process | Do not turn audit mechanics into template requirements |
| Dogfood preparation | Help define an observation rubric from the existing practice memo | Do not change the current templates before they are exercised |
| Dogfood execution | Read-only observer and available domain consultant through the existing Krab project | Do not become the hidden human cron or manually rescue ordinary transitions |
| Dogfood debrief | Compare expected and observed coordination work; classify friction | Do not select implementation shape during the debrief |
| Template contract milestone | Domain co-designer in a living, decision-tracked consultation | Product owner retains scope and authority decisions |
| Template implementation | Fidelity reviewer for the accepted practice contract | A separate reviewer supplies independent technical and security approval |

This sequencing avoids two opposite errors: designing only from one
coordinator's habits, and ignoring expensive lessons already learned in live
coordination. It also prevents the same participant from being both principal
designer and the sole independent reviewer of the implementation.

## Dogfood observation rubric

Before the first self-hosted run, MGB and the coordinator should agree on a
small observation sheet. It records facts without changing the run:

- every manual wake-up, reminder, retry, or status check;
- every transition the supervisor advanced without intervention;
- every point where authority or ownership was unclear;
- time spent waiting versus time spent making a judgment;
- changes-requested delivery and whether the original producer resumed with
  sufficient context;
- exact-head review, landing, refold, and closeout behavior;
- any follow-up discovered without widening the active work item;
- budget, quota, and remote-runner friction; and
- any place an operator reached for provider-native or repository-native
  state because the normalized MGB surface was insufficient.

The observation log belongs beside the dogfood report and contains event and
object identities, elapsed time, and bounded summaries—not private prompts,
credentials, message bodies, or model reasoning.

## Contract consultation

The formal consultation begins only after the dogfood report is frozen. It
uses a living internal document, with each question carrying MGB's position,
the coordinator's position, evidence pointers, an owner ruling, and a resolved
or deferred status. The initial agenda comes from the existing practice memo:

1. Does reuse mean one fixed delivery chain, independently invocable stage
   templates, portable configuration presets, or a deliberately small
   combination of those?
2. Does the fixed lifecycle live on a separate execution record or require a
   new work-item stage? It must not become a user-authored DAG.
3. When is one investigator sufficient, and when does ambiguity or severity
   require independent investigation?
4. Which coordinator actions are mechanical control-plane work, and which are
   irreducible scope, severity, or policy judgments?
5. Are gate review and multi-agent advisory review separate templates, or may
   one invoke the other under an explicit high-risk policy?
6. Can a mechanical test/docs-only delta carry prior evidence, and what exact
   allowlist and invalidation rules would make that safe?
7. Who owns budgets and review-round ceilings, and which durable ruling opens
   when either is exhausted?
8. Which events deserve built-in hooks: verdict delivery, head movement, base
   advancement, stalled work, quota exhaustion, landing, follow-up creation,
   or message handling?
9. What is the smallest reusable unit that can move between projects without
   carrying profile IDs, credentials, provider mappings, repository paths, or
   Panopticon-specific test machinery?
10. What exact tuple does maintainer approval bind, and which later proposal,
    provider-target, or landed-commit metadata changes invalidate it?
11. Who detects a dead coordinator, how long may it remain silent, and which
    durable event wakes a replacement? Recovery alone is insufficient.
12. May review depth scale with trusted risk and change shape? Which authority
    selects it, and how is producer influence excluded?
13. Does a reviewer approve code-as-read, code-as-run, or a typed combination,
    and which project-local toolchain evidence may it rely on?
14. How are provider-shaped return records and imported helper assumptions
    enumerated, translated, and tested at the portable/local boundary?
15. Does `landed` mean one provider receipt, independently observed default
    branch state, every configured mirror, or distinct typed states?

No schema or implementation is authorized merely because the consultation
records a useful idea. The contract milestone ends with a bounded decision
record, typed artifact and authority tables, negative cases, migration ruling,
and a deliberately small implementation recommendation.

The consultation opened on 2026-11-02 in Krab session
`ses_22d2b04b61b448d15079370c68b727dd`. Its living internal ledger freezes the
practice memo, current fixed-prescription contract, M16 postmortem, and this
program by content digest. The owner accepted the reconciled foundation: keep
fixed prescriptions rather than a meta-executor or work-item stage; derive one
total current-responsibility fact from exact artifact revisions; retry only
trusted policy-classified failures; and separate product disposition, project
landing policy, and derived landing readiness. Mechanical landing need not use
a different actor after independent review and owner disposition. No
ceiling-exception class is pre-delegated. A typed cosmetic metadata amendment
may remain maintainer-only, while ambiguous or claim-significant changes require
renewed disposition and independent review.

The owner also accepted the review-shape decisions. Discovery keeps one
investigator and coordinator, permits later producer reuse without amendment
authority, and records an exact skip when policy does not require it. Gate and
advisory evidence remain blind and separate; a smaller measurement review is a
future fixed topology rather than an opaque extra seat. A mechanical refold is
a typed machine attestation, never a reviewer verdict, and its implementation
is deferred unless the existing fold ledger makes it negligible. Risk tiers
combine an evidence floor with resource ceilings including shared-host
concurrency, and do not permit unbounded delivery time. One lineage ceiling
covers review and maintainer corrections with a cause recorded for every round;
proven refolds and validation/provider retries remain separately accounted.

The owner also accepted the lifecycle contract. Safety transitions remain
deterministic while materiality and cause stay role judgments; prompt item
closeout is separate from target-wide breadth verification. Wake bindings must
be observed, availability is distinct from acknowledgement, and M18 remains
explicitly manual until fenced watchdog replacement is proven. Follow-ups are
self-contained non-authoritative proposals with bounded coordinator admission;
a coordinator may place a landing-target hold, but only the owner may lift it.
Reviewer measurements run without credentials or effect authority and never
satisfy immutable validation. The first contract supports explicit per-
obligation deferred validation, not a general waiver.

The final bounded wave now covers provider/local translation, effect
reconciliation, the meaning of landed, mirrors, open obligations, migration,
and M18's smallest accepted slice. No consultation decision has yet authorized
a production change.

## Reuse boundary

Reusable material may include:

- fixed stage topology and typed exit artifacts;
- required separation and provenance rules;
- budget and round-limit parameter shapes;
- lifecycle-event semantics and default supervisor reactions;
- provider-neutral capability requirements; and
- portable policy defaults that are reduced or bound by project-local
  configuration.

Reusable material may not contain:

- agent-profile IDs, Krab identities, credentials, SSH destinations, or model
  routes;
- GitHub, GitLab, Bitbucket, Azure DevOps, Jira, or Panopticon command syntax;
- repository paths, issue keys, test-kit paths, or known local failures;
- arbitrary expressions, branches, loops, callbacks, or user-authored graphs;
  or
- prompt prose presented as authority or completion evidence.

Project activation must bind the reusable contract to exact local profiles,
capabilities, providers, limits, and owner policy. If a required binding is
absent, the template is unavailable rather than partially enabled.

## Implementation and proof boundary

The first implementation milestone takes only the smallest accepted contract
slice. RED-first happy and sad paths, the 85% branch-aware coverage floor, and
focused mutation checks apply normally. Its live proof must run the same
reusable contract in two materially different projects—initially MGB and
Imitation Krab are the natural owned candidates—without embedding either
project's provider or test mechanics.

The proof must preserve producer/reviewer separation, exact-head authority,
genuine changes-requested return when one occurs, restart recovery, bounded
budgets, maintainer authority, and truthful landing/closeout. It must not
manufacture a rejection or widen a work item merely to demonstrate a branch.

After that proof, the Panopticon coordinator reviews fidelity to the accepted
practice contract. A separate blind review and the ordinary quality/security
gates decide whether the implementation lands.
