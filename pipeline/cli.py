"""Command-line entry point. Airflow calls these commands; so can you.

  pipeline ingest    --date 2026-09-01 [--source generate|platform] [--scale 1]
  pipeline quality   --date 2026-09-01 --layer raw|curated
  pipeline transform --date 2026-09-01 [--engine duckdb|spark]
  pipeline dbt
  pipeline run       --date 2026-09-01          (ingest, checks, transform, checks)
  pipeline backfill  --start 2026-09-01 --end 2026-09-07
  pipeline verify-idempotency --date 2026-09-01
  pipeline bench     --scales 1,10,100 --engines duckdb,spark

Exit code is non-zero when a step or a failing quality check should stop the pipeline.
"""

import argparse
import json
import sys
from datetime import date
from typing import Any

from pipeline import dbt_runner, runner, transform
from pipeline.config import Settings


def _print(obj: Any) -> None:
    print(json.dumps(obj, indent=2, default=str))


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)

    def day_cmd(name: str) -> argparse.ArgumentParser:
        sp = sub.add_parser(name)
        sp.add_argument("--date", type=date.fromisoformat, required=True)
        return sp

    for name in ("ingest", "run", "verify-idempotency"):
        sp = day_cmd(name)
        sp.add_argument("--source", choices=["generate", "platform"], default="generate")
        sp.add_argument("--scale", type=int, default=1)
        sp.add_argument("--seed", type=int, default=0)
        if name != "ingest":
            sp.add_argument("--engine", choices=["duckdb", "spark"], default="duckdb")
    day_cmd("quality").add_argument("--layer", choices=["raw", "curated"], required=True)
    day_cmd("transform").add_argument("--engine", choices=["duckdb", "spark"], default="duckdb")
    sp = sub.add_parser("dbt")
    sp.add_argument("--select")
    sp = sub.add_parser("backfill")
    sp.add_argument("--start", type=date.fromisoformat, required=True)
    sp.add_argument("--end", type=date.fromisoformat, required=True)
    sp.add_argument("--source", choices=["generate", "platform"], default="generate")
    sp.add_argument("--engine", choices=["duckdb", "spark"], default="duckdb")
    sp.add_argument("--scale", type=int, default=1)
    sp = sub.add_parser("bench")
    sp.add_argument("--scales", default="1,10,100")
    sp.add_argument("--engines", default="duckdb,spark")
    sp.add_argument("--out", default="results/bench")

    args = p.parse_args(argv)
    s = Settings().resolved()
    try:
        if args.cmd == "ingest":
            _print(runner.ingest(s, args.date, args.source, args.scale, args.seed))
        elif args.cmd == "quality":
            report = runner.check(s, args.layer, args.date)
            _print({"status": report["status"]})
        elif args.cmd == "transform":
            _print(transform.run(args.engine, s.lake, args.date))
        elif args.cmd == "dbt":
            result = dbt_runner.build(s, args.select)
            _print(result)
            return 1 if result["status"] in ("fail", "error") else 0
        elif args.cmd == "run":
            _print(runner.run_day(s, args.date, args.source, args.engine, args.scale, args.seed))
        elif args.cmd == "backfill":
            result = runner.backfill(
                s, args.start, args.end, source=args.source, engine=args.engine, scale=args.scale
            )
            _print({"days": len(result["days"]), "dbt": result["dbt"]})
            return 1 if result["dbt"]["status"] in ("fail", "error") else 0
        elif args.cmd == "verify-idempotency":
            result = runner.verify_idempotency(
                s,
                args.date,
                source=args.source,
                engine=args.engine,
                scale=args.scale,
                seed=args.seed,
            )
            _print(result)
            return 0 if result["identical"] else 1
        elif args.cmd == "bench":
            from pipeline import bench

            _print(
                bench.run(
                    s, [int(x) for x in args.scales.split(",")], args.engines.split(","), args.out
                )
            )
    except runner.QualityFailure as exc:
        print(f"QUALITY FAILURE: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
