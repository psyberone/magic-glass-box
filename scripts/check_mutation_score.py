#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from decimal import Decimal, InvalidOperation, localcontext
from fractions import Fraction
from pathlib import Path

MAX_STATS_BYTES = 1024 * 1024
MUTANT_STATUS_FIELDS = (
    "check_was_interrupted_by_user",
    "killed",
    "no_tests",
    "segfault",
    "skipped",
    "suspicious",
    "survived",
    "timeout",
)


class StatsError(ValueError):
    pass


def load_score(path: Path) -> tuple[int, int]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise StatsError(f"cannot read {path}: {exc}") from exc
    if not raw or len(raw) > MAX_STATS_BYTES:
        raise StatsError("stats file must contain between 1 byte and 1 MiB")
    try:
        payload = json.loads(raw.decode("utf-8", "strict"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise StatsError("stats file is not valid UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise StatsError("stats payload must be an object")
    if any(field not in payload for field in MUTANT_STATUS_FIELDS):
        raise StatsError("stats payload is missing mutant outcome counts")
    statuses: dict[str, int] = {}
    for field in MUTANT_STATUS_FIELDS:
        value = payload[field]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise StatsError("mutant outcome counts must be non-negative integers")
        statuses[field] = value
    not_checked = payload.get("not_checked", 0)
    if (
        isinstance(not_checked, bool)
        or not isinstance(not_checked, int)
        or not_checked < 0
    ):
        raise StatsError("not_checked must be a non-negative integer")
    total = payload.get("total")
    if (
        isinstance(total, bool)
        or not isinstance(total, int)
        or total <= 0
        or sum(statuses.values()) + not_checked != total
    ):
        raise StatsError("total must equal every reported mutant outcome")
    return statuses["killed"], total


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="enforce Mutmut's kill-rate floor")
    parser.add_argument("--stats", type=Path, required=True)
    parser.add_argument("--minimum", default="80")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        minimum = Decimal(args.minimum)
    except (InvalidOperation, ValueError):
        minimum = Decimal("NaN")
    if not minimum.is_finite() or not Decimal(0) <= minimum <= Decimal(100):
        print(
            "invalid mutation stats: minimum must be between 0 and 100", file=sys.stderr
        )
        return 2
    try:
        killed, total = load_score(args.stats)
    except StatsError as exc:
        print(f"invalid mutation stats: {exc}", file=sys.stderr)
        return 2
    threshold = Fraction(minimum)
    below_floor = killed * 100 * threshold.denominator < threshold.numerator * total
    with localcontext() as context:
        context.prec = max(28, len(str(killed)), len(str(total))) + 4
        score = Decimal(killed) * 100 / Decimal(total)
    message = f"mutation kill rate: {score:.2f}% ({killed}/{total})"
    if below_floor:
        print(f"{message}; below required {minimum:.2f}%", file=sys.stderr)
        return 1
    print(f"{message}; required {minimum:.2f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
