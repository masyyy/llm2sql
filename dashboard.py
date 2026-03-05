"""Streamlit dashboard for LLM2SQL benchmark results."""

import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

DATA_DIR = Path(__file__).parent / "data"


def load_results() -> list[dict]:
    """Load all benchmark result JSON files."""
    results = []
    for f in sorted(DATA_DIR.glob("results_*.json")):
        data = json.loads(f.read_text())
        results.append(data)
    return results


def main():
    st.set_page_config(page_title="LLM2SQL Benchmark", layout="wide")
    st.title("LLM2SQL Benchmark Dashboard")

    results = load_results()
    if not results:
        st.warning("No results found in data/. Run the benchmark first.")
        return

    # --- Summary cards ---
    cols = st.columns(len(results))
    for i, res in enumerate(results):
        with cols[i]:
            st.subheader(res["model_name"])
            st.metric("Execution Accuracy", f"{res['execution_accuracy']:.1%}")
            st.metric("Result Match Avg", f"{res['result_match_avg']:.1%}")
            passed = sum(1 for r in res["results"] if r["result_match"] == 1.0)
            st.metric("Queries Passed", f"{passed}/{res['total_queries']}")

    # --- Per-query comparison bar chart ---
    st.subheader("Result Match by Query")
    chart_data = []
    for res in results:
        for r in res["results"]:
            chart_data.append({
                "Query": f"Q{r['query_id']}",
                "Model": res["model_name"],
                "Result Match": r["result_match"],
                "Difficulty": r["difficulty"],
            })
    df = pd.DataFrame(chart_data)

    fig = px.bar(
        df, x="Query", y="Result Match", color="Model",
        barmode="group", range_y=[0, 1.1],
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    st.plotly_chart(fig, use_container_width=True)

    # --- By difficulty ---
    st.subheader("Average Score by Difficulty")
    diff_data = df.groupby(["Difficulty", "Model"])["Result Match"].mean().reset_index()
    diff_order = {"easy": 0, "medium": 1, "hard": 2}
    diff_data["sort"] = diff_data["Difficulty"].map(diff_order)
    diff_data = diff_data.sort_values("sort")

    fig2 = px.bar(
        diff_data, x="Difficulty", y="Result Match", color="Model",
        barmode="group", range_y=[0, 1.1],
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    st.plotly_chart(fig2, use_container_width=True)

    # --- Detail table ---
    st.subheader("Query Details")
    for res in results:
        with st.expander(f"{res['model_name']} — detailed results"):
            for r in res["results"]:
                status = "Pass" if r["result_match"] == 1.0 else (
                    "Partial" if r["result_match"] == 0.5 else "Fail"
                )
                icon = {"Pass": "✅", "Partial": "⚠️", "Fail": "❌"}[status]

                with st.expander(
                    f"{icon} Q{r['query_id']} [{r['difficulty']}] — {r['question'][:80]}"
                ):
                    c1, c2 = st.columns(2)
                    with c1:
                        st.caption("Ground Truth SQL")
                        st.code(r["ground_truth_sql"], language="sql")
                    with c2:
                        st.caption("Generated SQL")
                        st.code(r["generated_sql"] or "(empty)", language="sql")

                    if r["execution_error"]:
                        st.error(f"Execution error: {r['execution_error']}")

                    st.caption(f"Match score: {r['result_match']} | "
                               f"GT rows: {len(r['ground_truth_rows'])} | "
                               f"Gen rows: {len(r['generated_rows'])}")


if __name__ == "__main__":
    main()
