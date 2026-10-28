# Practice-informed reusable workflow templates

**Status: Milestone 17 contract design and Milestone 18 first delivery, after
the post-M15 audit and controlled M16 dogfood run.**

MGB already implements fixed, versioned `discovery-v1`, `implementation-v1`,
and `advisory-review-v1` prescriptions. The next template work is not a request
for a general workflow builder. It is a bounded effort to determine which
parts of repeatedly successful coordination practice should become reusable
product contracts, which should remain project configuration, and which are
Panopticon-specific and must stay outside MGB.

The Panopticon coordinator has already supplied a durable practice memo drawn
from running a large producer/reviewer/landing operation by hand. That memo is
an evidence source, not an automatic specification. MGB's own dogfood run must
provide the second evidence source before design decisions are frozen.

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

No schema or implementation is authorized merely because the consultation
records a useful idea. The contract milestone ends with a bounded decision
record, typed artifact and authority tables, negative cases, migration ruling,
and a deliberately small implementation recommendation.

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
