"""
================================================================================
  RAG Evaluation Script — RAGAS metrics
  ---------------------------------------------------------------------------
  Metrics
  -------
  faithfulness       : Are the answer's claims supported by the retrieved
                       context?  Catches LLM hallucination.
                       Score: fraction of answer claims that can be inferred
                       from the context chunks.  Range [0, 1].

  answer_relevancy   : Does the answer actually address the question?
                       Reverse-engineers questions from the answer, then
                       measures cosine similarity to the original question.
                       Catches off-topic answers even when they are factual.
                       Range [0, 1].

  context_precision  : Are the relevant chunks ranked near the top of the
                       retrieved list?  Uses ground_truth to label which
                       chunks are relevant, then computes mean precision@k.
                       Penalises retrievers that bury the right chunk at k=4.
                       Range [0, 1].

  context_recall     : Does the retrieved context cover everything the
                       ground_truth answer requires?  Decomposes ground_truth
                       into claims, checks each against context.
                       Range [0, 1].

  Usage
  -----
      python evaluate_rag.py                          # all test cases
      python evaluate_rag.py --category reliability   # one category only
      python evaluate_rag.py --ids arch_001 speed_004 # specific IDs
      python evaluate_rag.py --dry-run                # print questions only
      python evaluate_rag.py --skip-negative          # exclude negative cases
================================================================================
"""

import argparse
import json
import os
import sys
from pathlib import Path

import pandas as pd
from datasets import Dataset
from ragas import evaluate
from ragas.metrics import (
    faithfulness,
    answer_relevancy,
    context_precision,
    context_recall,
)

from rag_pipeline import init_rag, run_query, OPENAI_API_KEY


DATASET_PATH    = Path(__file__).parent / "test_dataset.json"
BASE_METRICS    = [faithfulness, answer_relevancy, context_precision]
RECALL_METRIC   = context_recall   # expensive — opt-in via --with-recall


# ── data loading ──────────────────────────────────────────────────────────────

def load_dataset(path: Path = DATASET_PATH) -> list[dict]:
    with open(path) as f:
        return json.load(f)["test_cases"]


def load_case(case_id: str, path: Path = DATASET_PATH) -> dict:
    cases = load_dataset(path)
    match = next((tc for tc in cases if tc["id"] == case_id), None)
    if match is None:
        raise KeyError(f"No test case with id '{case_id}'. Available: {[tc['id'] for tc in cases]}")
    return match


# ── pipeline execution ────────────────────────────────────────────────────────

def collect_rag_outputs(test_cases: list[dict], chain) -> list[dict]:
    """
    Run every test case through the RAG pipeline and collect the four fields
    that RAGAS needs: question, answer, contexts, ground_truth.

    contexts is a list[str] — one string per retrieved chunk.
    ground_truth is the expected_answer from the test dataset.
    """
    rows = []
    for i, tc in enumerate(test_cases, 1):
        print(f"  [{i}/{len(test_cases)}] {tc['id']}  — {tc['question'][:70]}...")
        result = run_query(chain, tc["question"])

        rows.append({
            # RAGAS required fields
            "question":     tc["question"],
            "answer":       result["answer"],
            "contexts":     [s["content"] for s in result["sources"]],
            "ground_truth": tc["expected_answer"],
            # metadata kept for the report
            "id":           tc["id"],
            "category":     tc["category"],
            "difficulty":   tc["difficulty"],
        })

    return rows


# ── RAGAS evaluation ──────────────────────────────────────────────────────────

def run_ragas(rows: list[dict], *, with_recall: bool = False) -> pd.DataFrame:
    """
    Build a HuggingFace Dataset from the collected rows and run RAGAS.
    Returns a DataFrame with one row per test case and one column per metric.

    with_recall : include context_recall (more LLM calls, higher cost).
    """
    metrics = BASE_METRICS + ([RECALL_METRIC] if with_recall else [])

    # RAGAS only looks at these four columns
    ragas_dataset = Dataset.from_list([
        {
            "question":     r["question"],
            "answer":       r["answer"],
            "contexts":     r["contexts"],
            "ground_truth": r["ground_truth"],
        }
        for r in rows
    ])

    # RAGAS auto-detects the LLM from OPENAI_API_KEY in the environment.
    # Set it explicitly here so this function is self-contained — it must not
    # rely on init_rag() having been called first.
    os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY

    metric_names = " | ".join(m.name for m in metrics)
    print(f"\nRunning RAGAS evaluation [{metric_names}] (this makes additional LLM calls) ...")
    result = evaluate(ragas_dataset, metrics=metrics)

    scores_df = result.to_pandas()

    # attach metadata columns
    scores_df["id"]         = [r["id"]         for r in rows]
    scores_df["category"]   = [r["category"]   for r in rows]
    scores_df["difficulty"] = [r["difficulty"] for r in rows]

    return scores_df


# ── reporting ─────────────────────────────────────────────────────────────────

BASE_METRIC_COLS   = ["faithfulness", "answer_relevancy", "context_precision"]
RECALL_METRIC_COL  = "context_recall"


def _metric_cols(df: pd.DataFrame) -> list[str]:
    """Return whichever metric columns are present in df."""
    return [c for c in BASE_METRIC_COLS + [RECALL_METRIC_COL] if c in df.columns]


def print_per_case(df: pd.DataFrame) -> None:
    metric_cols = _metric_cols(df)
    print(f"\n{'='*100}")
    print("  PER-CASE SCORES")
    print(f"{'='*100}")
    print(df[["id", "difficulty"] + metric_cols].to_string(index=False, float_format="{:.3f}".format))


def print_summary(df: pd.DataFrame) -> None:
    metric_cols = _metric_cols(df)

    print(f"\n{'='*100}")
    print("  OVERALL AVERAGES")
    print(f"{'='*100}")
    print(df[metric_cols].mean().to_string(float_format="{:.3f}".format))

    print(f"\n{'='*100}")
    print("  PER-CATEGORY AVERAGES")
    print(f"{'='*100}")
    print(df.groupby("category")[metric_cols].mean().to_string(float_format="{:.3f}".format))

    print(f"\n{'='*100}")
    print("  PER-DIFFICULTY AVERAGES")
    print(f"{'='*100}")
    print(df.groupby("difficulty")[metric_cols].mean().to_string(float_format="{:.3f}".format))

    # flag low-scoring cases (any metric < 0.5)
    low = df[df[metric_cols].min(axis=1) < 0.5][["id", "difficulty"] + metric_cols]
    if not low.empty:
        print(f"\n{'='*100}")
        print("  LOW-SCORING CASES  (any metric < 0.5)")
        print(f"{'='*100}")
        print(low.to_string(index=False, float_format="{:.3f}".format))


def save_results(df: pd.DataFrame, rows: list[dict], path: Path) -> None:
    """Save full results including the model's answer and retrieved chunks."""
    metric_cols = _metric_cols(df)
    output = []
    for _, row in df.iterrows():
        meta = next(r for r in rows if r["id"] == row["id"])
        record = {
            "id":           row["id"],
            "category":     row["category"],
            "difficulty":   row["difficulty"],
            "question":     meta["question"],
            "ground_truth": meta["ground_truth"],
            "answer":       meta["answer"],
            "contexts":     meta["contexts"],
        }
        for col in metric_cols:
            record[col] = round(float(row[col]), 3)
        output.append(record)

    with open(path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n  Full results saved to: {path}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Evaluate the WiFi KPI RAG pipeline with RAGAS.")
    p.add_argument("--category",       help="Filter by category (e.g. reliability)")
    p.add_argument("--ids", nargs="+", help="Run specific test case IDs")
    p.add_argument("--skip-negative",  action="store_true",
                   help="Exclude negative test cases (id starts with 'negative')")
    p.add_argument("--dry-run",        action="store_true",
                   help="Print questions without calling RAG or RAGAS")
    p.add_argument("--with-recall", action="store_true",
                   help="Include context_recall metric (higher cost — extra LLM calls)")
    p.add_argument("--top-k",   type=int, default=5,
                   help="Chunks to retrieve per query (default: 5)")
    p.add_argument("--output",  default="eval_results.json",
                   help="Output JSON file (default: eval_results.json)")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    # load all cases, or a single case directly by id
    if args.ids and len(args.ids) == 1:
        test_cases = [load_case(args.ids[0])]
    else:
        test_cases = load_dataset()
        if args.ids:
            test_cases = [tc for tc in test_cases if tc["id"] in args.ids]
    if args.category:
        test_cases = [tc for tc in test_cases if tc["category"] == args.category]
    if args.skip_negative:
        test_cases = [tc for tc in test_cases if not tc["id"].startswith("negative")]

    if not test_cases:
        print("No test cases matched the filters.")
        sys.exit(0)

    active = "faithfulness | answer_relevancy | context_precision"
    if args.with_recall:
        active += " | context_recall"
    print(f"Evaluating {len(test_cases)} test case(s) with RAGAS metrics:\n  {active}\n")

    if args.dry_run:
        for tc in test_cases:
            print(f"  [{tc['id']}] {tc['question']}")
        return

    # stage 1: collect RAG outputs
    print("Running RAG pipeline ...")
    chain = init_rag(top_k=args.top_k)
    rows  = collect_rag_outputs(test_cases, chain)

    # stage 2: RAGAS scoring
    scores_df = run_ragas(rows, with_recall=args.with_recall)

    # stage 3: report
    print_per_case(scores_df)
    print_summary(scores_df)

    output_path = Path(__file__).parent / args.output
    save_results(scores_df, rows, output_path)


if __name__ == "__main__":
    main()
