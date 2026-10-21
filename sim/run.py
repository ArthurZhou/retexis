#!/usr/bin/env python3
"""Simulator CLI.

    python sim/run.py --list
    python sim/run.py all
    python sim/run.py storm --verbose
    python sim/run.py loss reconnect --json

Exit code is 0 only if every check passed, so this works in CI.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scenarios import SCENARIOS, run_scenario  # noqa: E402


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        prog="sim",
        description="retexis protocol simulator — topologies, loss, storms, auth",
    )
    ap.add_argument("scenario", nargs="*", help="scenario name(s), or 'all'")
    ap.add_argument("--list", action="store_true", help="list available scenarios")
    ap.add_argument("--json", action="store_true", help="machine readable output")
    ap.add_argument("--verbose", action="store_true", help="dump the event log")
    args = ap.parse_args(argv)

    if args.list:
        width = max(len(n) for n in SCENARIOS)
        for name, fn in SCENARIOS.items():
            doc = (fn.__doc__ or "").strip().splitlines()[0]
            print(f"  {name:<{width}}  {doc}")
        return 0

    names = args.scenario or ["all"]
    if "all" in names:
        names = list(SCENARIOS)

    results = []
    for name in names:
        if name not in SCENARIOS:
            print(f"unknown scenario '{name}' (try --list)", file=sys.stderr)
            return 2
        results.append(run_scenario(name, verbose=args.verbose))

    if args.json:
        payload = [
            {
                "name": r.name,
                "title": r.title,
                "ok": r.ok,
                "checks": [
                    {"label": label, "ok": passed, "detail": detail}
                    for label, passed, detail in r.checks
                ],
                "metrics": r.metrics,
                "notes": r.notes,
            }
            for r in results
        ]
        print(json.dumps(payload, indent=2, default=str))
    else:
        for r in results:
            print()
            print(f"{'PASS' if r.ok else 'FAIL'}  {r.name}  —  {r.title}")
            for label, passed, detail in r.checks:
                mark = "ok  " if passed else "FAIL"
                tail = f"   {detail}" if detail else ""
                print(f"  [{mark}] {label}{tail}")
            if r.metrics:
                flat = ", ".join(f"{k}={v}" for k, v in r.metrics.items())
                print(f"  metrics: {flat}")
            for note in r.notes:
                print(f"  note:    {note}")
        total = len(results)
        failed = [r for r in results if not r.ok]
        print()
        print(f"{total - len(failed)}/{total} scenarios passed")
        for r in failed:
            for label, detail in r.failures():
                print(f"  failed: {r.name}: {label} ({detail})")

    return 0 if all(r.ok for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
