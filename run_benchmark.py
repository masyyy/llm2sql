"""CLI runner for the LLM2SQL benchmark."""

import argparse
import json
import sys
from datetime import datetime

from llm2sql.config import DB_PATH, DATA_DIR
from llm2sql.models import BenchmarkResult
from llm2sql.queries import QUERIES
from llm2sql.prompt import build_system_prompt
from llm2sql.inference import BACKENDS
from llm2sql.evaluate import score_query
from llm2sql.seed import setup as setup_db


def cmd_setup_db():
    setup_db()


def cmd_run(model: str, verbose: bool = False):
    if model not in BACKENDS:
        print(f"Unknown model: {model}. Available: {', '.join(BACKENDS)}")
        sys.exit(1)

    if not DB_PATH.exists():
        print("Database not found. Run 'setup-db' first.")
        sys.exit(1)

    backend = BACKENDS[model]
    system_prompt = build_system_prompt()
    results = []

    print(f"Running benchmark with {model}...")
    for q in QUERIES:
        qid = q["id"]
        print(f"  Q{qid:2d}: {q['question'][:60]}...", end=" " if not verbose else "\n", flush=True)

        try:
            generated_sql, raw_response = backend(system_prompt, q["question"])
        except Exception as e:
            print(f"INFERENCE ERROR: {e}")
            generated_sql, raw_response = "", ""

        if verbose:
            print(f"  --- RAW RESPONSE ---")
            print(f"  {raw_response}")
            print(f"  --- EXTRACTED SQL ---")
            print(f"  {generated_sql}")
            print(f"  ---------------------")

        result = score_query(
            query_id=qid,
            question=q["question"],
            difficulty=q["difficulty"],
            ground_truth_sql=q["sql"],
            generated_sql=generated_sql,
            db_path=str(DB_PATH),
        )
        results.append(result)

        status = "MATCH" if result.result_match == 1.0 else (
            "PARTIAL" if result.result_match == 0.5 else (
                "FAIL" if result.execution_success else "ERROR"
            )
        )
        print(f"  {'=> ' if verbose else ''}{status}")

    exec_acc = sum(1 for r in results if r.execution_success) / len(results)
    match_avg = sum(r.result_match for r in results) / len(results)

    benchmark = BenchmarkResult(
        model_name=model,
        timestamp=datetime.now().isoformat(),
        total_queries=len(results),
        execution_accuracy=round(exec_acc, 4),
        result_match_avg=round(match_avg, 4),
        results=results,
    )

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DATA_DIR / f"results_{model.replace('.', '_')}.json"
    out_path.write_text(benchmark.model_dump_json(indent=2))

    print(f"\n{'='*50}")
    print(f"Model: {model}")
    print(f"Execution accuracy: {exec_acc:.1%}")
    print(f"Result match avg:   {match_avg:.1%}")
    print(f"Results saved to:   {out_path}")


def main():
    parser = argparse.ArgumentParser(description="LLM2SQL Benchmark Runner")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("setup-db", help="Create and seed the benchmark database")

    run_parser = sub.add_parser("run", help="Run benchmark for a model")
    run_parser.add_argument("--model", required=True, choices=list(BACKENDS.keys()),
                            help="Model to benchmark")
    run_parser.add_argument("--verbose", "-v", action="store_true",
                            help="Print raw LLM responses and extracted SQL")

    args = parser.parse_args()

    if args.command == "setup-db":
        cmd_setup_db()
    elif args.command == "run":
        cmd_run(args.model, verbose=args.verbose)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
