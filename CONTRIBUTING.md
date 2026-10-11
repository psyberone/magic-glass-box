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

Run the fast gate while developing:

```console
make quality
```

The project requires at least **85% branch coverage** over the
`magic_glass_box` package. Coverage is a floor and a gap-finding tool, not a
substitute for assertions that demonstrate useful behavior.

Run mutation testing before merging a substantial behavior change:

```console
make mutation
```

Mutmut is limited to source lines exercised by tests. Its exported result must
have a literal kill rate of at least **80%**: `killed / total`. Untested,
skipped, suspicious, timed-out, and surviving mutants therefore lower the
score. Equivalent mutants should be documented and excluded narrowly in code;
do not weaken the project-wide threshold.

## End-of-phase review gate

A milestone is not complete when its implementation first goes green. At the
end of each phase:

1. Run the quality, mutation, and applicable live-proof gates.
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
