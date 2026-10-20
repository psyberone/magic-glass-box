# Contributing

## Behavior changes are RED-first

For every behavior change:

1. Add or modify a test that describes the intended behavior.
2. Include the meaningful happy path and sad paths at the same boundary.
3. Run the focused test and observe it fail for the expected reason (**RED**).
4. Make the smallest production change that satisfies the test (**GREEN**).
5. Refactor only while the suite remains green.

A test that passes before the behavior change does not establish the RED step.
Bug fixes start with a reproducer. Test-only, documentation, and mechanical
changes should still be verified in proportion to their risk.

## Quality gates

Create the development environment once, then use the Makefile entry points:

```console
python3.11 -m venv .venv
.venv/bin/python -m pip install -e '.[development]'
make test
```

Every gate defaults to `.venv/bin/python`, so the test runner, Ruff, coverage,
Mutmut, and the mutation-score checker use one dependency set. An explicit
interpreter remains supported, for example `make PYTHON=/opt/python/bin/python
test`. The mutation command is independently overridable and may contain
arguments, for example `make MUTMUT='uv run mutmut' mutation`. On a machine
with sufficient CPU and memory, Mutmut's worker count can be selected explicitly
without changing the portable default:

```console
make MUTMUT_RUN_ARGS='--max-children 20' mutation-clean
```

Run the fast gate while developing:

```console
make quality
```

Provider-neutral service-sidecar changes also have a focused contract,
persistence, orchestration, and real-Unix proof:

```console
make service-sidecar-test
```

The project requires at least **85% aggregate statement-and-branch coverage**
over the `magic_glass_box` package, using Coverage.py's combined percentage
with branch measurement enabled. Coverage is a floor and a gap-finding tool,
not a substitute for assertions that demonstrate useful behavior.

Run mutation testing before merging a substantial behavior change:

```console
make mutation
```

For the final end-of-phase gate, start from a fresh generated workspace:

```console
make mutation-clean
```

The clean target removes only ignored Mutmut output and Python bytecode caches
before running the same score gate. This prevents covered-line discovery from
observing stale transformed modules after source layout changes; the ordinary
target remains useful for faster iteration.

Mutmut is limited to source lines exercised by tests. Its exported result must
have a literal kill rate of at least **80%**: `killed / total`. Untested,
skipped, suspicious, timed-out, and surviving mutants therefore lower the
score. Equivalent mutants should be documented and excluded narrowly in code;
do not weaken the project-wide threshold.

## End-of-phase review gate

A milestone is not complete when its implementation first goes green. At the
end of each phase:

1. Run the quality, clean-mutation, and applicable live-proof gates.
2. Freeze the candidate at one exact Git commit.
3. Have an independent reviewer inspect the whole milestone scope at that
   commit, including its architecture, security boundaries, failure behavior,
   and tests.
4. Treat unresolved severity 0 or severity 1 findings as release blockers.
   Record any deliberately accepted lower-severity findings and their rationale.
5. Start fixes with a RED regression test, rerun the affected gates, and review
   the resulting commit again before marking the milestone complete.

Review working notes belong in the ignored `docs/internal/` directory. The
tracked product and contributor documentation must still contain every durable
decision needed to understand or verify the shipped behavior.

## Review against the owned threat boundary

Use the architecture rule **outside scary, inside safe** when proposing or
reviewing defensive code. Inputs crossing from repositories, workers, imports,
external services, and sidecars are untrusted and must be validated at their
boundary. State already published beneath MGB's private control-plane root is
trusted and should use normal operating-system, SQLite, digest, atomic-write,
and locking guarantees.

Do not add complexity solely to model a same-UID attacker racing private MGB
files, a compromised operator account, or a hostile kernel/container runtime.
Those conditions have already defeated the application's trusted computing
base. A defensive branch should name the boundary it protects and a realistic
failure it prevents; otherwise it does not belong in the core.
