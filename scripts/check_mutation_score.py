#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sys
from collections.abc import Sequence
from pathlib import Path

MAX_STATS_BYTES = 1024 * 1024


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
    killed = payload.get("killed")
    total = payload.get("total")
    if (
        isinstance(killed, bool)
        or not isinstance(killed, int)
        or isinstance(total, bool)
        or not isinstance(total, int)
        or killed < 0
        or total <= 0
        or killed > total
    ):
        raise StatsError("killed and total must satisfy 0 <= killed <= total")
    return killed, total


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="enforce Mutmut's kill-rate floor")
    parser.add_argument("--stats", type=Path, required=True)
    parser.add_argument("--minimum", type=float, default=80.0)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not math.isfinite(args.minimum) or not 0 <= args.minimum <= 100:
        print(
            "invalid mutation stats: minimum must be between 0 and 100", file=sys.stderr
        )
        return 2
    try:
        killed, total = load_score(args.stats)
    except StatsError as exc:
        print(f"invalid mutation stats: {exc}", file=sys.stderr)
        return 2
    score = killed / total * 100
    message = f"mutation kill rate: {score:.2f}% ({killed}/{total})"
    if score < args.minimum:
        print(f"{message}; below required {args.minimum:.2f}%", file=sys.stderr)
        return 1
    print(f"{message}; required {args.minimum:.2f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
