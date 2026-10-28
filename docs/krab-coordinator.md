# Krab coordinator hooks

Magic Glass Box can use an operator-owned Imitation Krab identity without
copying its bearer token into the repository, the control-plane database, a
worker workspace, or command output. The control plane retains only a
`file:/...` credential reference and the stable Krab user ID returned by
`whoami`. Every later command resolves the private file, authenticates again,
and rejects a token that now belongs to a different user ID.

Krab remains authoritative for project membership, coordinator/member/admin
roles, session participation, item transitions, optimistic versions, and inbox
cursors. These hooks do not recreate that authority in MGB and do not shell out
to Krab's raw CLI.

## One-time binding

The token's parent directory must be private to the operator (`0700`) and the
regular token file must be private (`0600`). It may not be a symlink or a
multiply linked file.

```console
magic-glass-box init
magic-glass-box krab instance-add local-krab \
  --url http://127.0.0.1:8765
magic-glass-box krab identity-add local-krab \
  --token-file ~/.config/imitation-krab/coordinator.token
```

`identity-add` verifies the credential immediately. It stores the stable user
ID, cached handle, token version, and credential reference—not the token.

## Read-only operations

```console
magic-glass-box krab projects local-krab --as-user coordinator
magic-glass-box krab sessions local-krab magic-glass-box \
  --as-user coordinator
magic-glass-box krab inbox local-krab magic-glass-box \
  --as-user coordinator --unread --limit 50
magic-glass-box krab item-show local-krab magic-glass-box \
  ses_<id> itm_<id> --as-user coordinator
```

Project activity remains metadata-only. The identity inbox and item view are
deliberately private-content operations and return bodies or notes only to the
authenticated participant. MGB does not persist those bodies or notes as part
of these commands.

`inbox --unread` starts after Krab's durable acknowledgement cursor. An
explicit `--after N` is an independent cursor read; the two modes are mutually
exclusive. Reading never acknowledges an event.

## Explicit mutations

Every mutation requires a caller-chosen idempotency key. Repeating a request
after an ambiguous transport failure must reuse that exact key and payload.

```console
magic-glass-box krab session-create local-krab magic-glass-box \
  --as-user coordinator --label "M15 review" \
  --participant coordinator --participant reviewer \
  --idempotency-key mgb.session:<stable-key>

magic-glass-box krab send local-krab magic-glass-box ses_<id> \
  --as-user coordinator --to reviewer --title "Review candidate" \
  --body-file ./request.md --idempotency-key mgb.item:<stable-key>

magic-glass-box krab item-status local-krab magic-glass-box \
  ses_<id> itm_<id> --as-user coordinator \
  --status open.needs_changes --expected-version 2 \
  --note-file ./changes-requested.md \
  --idempotency-key mgb.status:<stable-key>

magic-glass-box krab inbox-ack local-krab magic-glass-box \
  --as-user coordinator --through 42 \
  --idempotency-key mgb.inbox:<stable-key>
```

Message bodies and review notes are file inputs so private text does not need
to appear in the process argument list. They are read once with Krab's byte
limits and strict UTF-8 validation. Status changes require the observed item
version; changes-requested and rejected outcomes require a note. Inbox
acknowledgement is separate and monotonic, so an operator can process an event
before marking it handled.

Closing a session is also explicit and idempotent. Krab permits it only for the
session creator or a project admin and only after every item is terminal.

## Intended use

This is an operator/coordinator edge over the existing typed Krab client. It is
not a worker capability: sandboxed agents continue to receive only their
run-scoped MGB gateway. It is also not an autonomous inbox consumer. A future
supervisor policy may decide when to read or acknowledge coordinator work, but
this surface makes those decisions possible without handing MGB or a worker an
unrestricted Krab command client.
