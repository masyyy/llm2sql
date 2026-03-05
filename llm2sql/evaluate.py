"""SQL execution and result comparison for benchmarking."""

import sqlite3

from llm2sql.models import QueryResult


def execute_sql(db_path: str, sql: str) -> tuple[bool, list[list], list[str], str | None]:
    """Execute SQL and return (success, rows, columns, error)."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.execute(sql)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = [list(row) for row in cursor.fetchall()]
        conn.close()
        return True, rows, columns, None
    except Exception as e:
        return False, [], [], str(e)


def _normalize_value(v):
    """Normalize a value for comparison."""
    if v is None:
        return None
    if isinstance(v, float):
        return round(v, 2)
    return v


def _normalize_rows(rows: list[list]) -> list[tuple]:
    """Sort rows and normalize values for comparison."""
    normalized = [tuple(_normalize_value(v) for v in row) for row in rows]
    return sorted(normalized)


def _project_rows(rows: list[list], indices: list[int]) -> list[tuple]:
    """Project rows to only include specified column indices."""
    return [tuple(_normalize_value(row[i]) for i in indices) for row in rows]


def _find_column_mapping(gt_cols: list[str], gen_cols: list[str]) -> list[tuple[int, int]]:
    """Find matching columns between ground truth and generated results.

    Returns list of (gt_index, gen_index) pairs.
    """
    # Canonicalize: lowercase, strip common prefixes
    def canonical(col: str) -> str:
        c = col.lower().strip()
        # Strip table-like prefixes: customer_name -> name, total_revenue_ytd -> revenue_ytd
        for prefix in ["customer_", "account_manager_", "total_", "product_"]:
            if c.startswith(prefix) and len(c) > len(prefix):
                alt = c[len(prefix):]
                # Only strip if what remains is meaningful (>2 chars)
                if len(alt) > 2:
                    return alt
        return c

    mapping = []
    used_gen = set()

    # Pass 1: exact match
    for gi, gc in enumerate(gt_cols):
        for geni, genc in enumerate(gen_cols):
            if geni in used_gen:
                continue
            if gc.lower() == genc.lower():
                mapping.append((gi, geni))
                used_gen.add(geni)
                break

    # Pass 2: canonical match for unmatched GT cols
    matched_gt = {m[0] for m in mapping}
    for gi, gc in enumerate(gt_cols):
        if gi in matched_gt:
            continue
        gc_canon = canonical(gc)
        for geni, genc in enumerate(gen_cols):
            if geni in used_gen:
                continue
            if canonical(genc) == gc_canon:
                mapping.append((gi, geni))
                used_gen.add(geni)
                break

    return mapping


def compare_results(
    gt_rows: list[list],
    gen_rows: list[list],
    gt_cols: list[str],
    gen_cols: list[str],
) -> float:
    """Compare ground truth and generated result rows.

    Matches columns by name, projects to overlapping columns, then checks
    if all GT rows appear in generated results. Extra columns and extra rows
    in generated results are not penalized.

    Returns:
        1.0 — all GT rows found in generated results
        0.5 — >50% of GT rows found
        0.0 — ≤50% match or no columns overlap
    """
    if not gt_rows:
        return 1.0 if not gen_rows else 0.0

    # Fast path: identical
    gt_norm = _normalize_rows(gt_rows)
    gen_norm = _normalize_rows(gen_rows)
    if gt_norm == gen_norm:
        return 1.0

    # Find overlapping columns
    mapping = _find_column_mapping(gt_cols, gen_cols)
    if not mapping:
        return 0.0

    gt_indices = [m[0] for m in mapping]
    gen_indices = [m[1] for m in mapping]

    gt_projected = _project_rows(gt_rows, gt_indices)
    gen_projected = _project_rows(gen_rows, gen_indices)

    # Exact match on projected columns
    if sorted(gt_projected) == sorted(gen_projected):
        return 1.0

    # Check how many GT rows appear in generated (with float tolerance)
    def _rows_close(a: tuple, b: tuple) -> bool:
        if len(a) != len(b):
            return False
        for va, vb in zip(a, b):
            if va is None and vb is None:
                continue
            if va is None or vb is None:
                return False
            if isinstance(va, (int, float)) and isinstance(vb, (int, float)):
                if abs(va - vb) > max(abs(va), abs(vb), 1) * 0.02:
                    return False
            elif va != vb:
                return False
        return True

    used_gen = [False] * len(gen_projected)
    matched = 0
    for gt_row in gt_projected:
        for i, gen_row in enumerate(gen_projected):
            if not used_gen[i] and _rows_close(gt_row, gen_row):
                used_gen[i] = True
                matched += 1
                break

    if matched == len(gt_projected):
        return 1.0

    ratio = matched / len(gt_projected)
    if ratio > 0.5:
        return 0.5

    return 0.0


def score_query(
    query_id: int,
    question: str,
    difficulty: str,
    ground_truth_sql: str,
    generated_sql: str,
    db_path: str,
) -> QueryResult:
    """Score a single generated SQL query against ground truth."""
    gt_ok, gt_rows, gt_cols, gt_err = execute_sql(db_path, ground_truth_sql)
    gen_ok, gen_rows, gen_cols, gen_err = execute_sql(db_path, generated_sql)

    if not gen_ok:
        return QueryResult(
            query_id=query_id,
            question=question,
            difficulty=difficulty,
            ground_truth_sql=ground_truth_sql.strip(),
            generated_sql=generated_sql.strip(),
            execution_success=False,
            execution_error=gen_err,
            ground_truth_rows=gt_rows,
            generated_rows=[],
            result_match=0.0,
        )

    match_score = compare_results(gt_rows, gen_rows, gt_cols, gen_cols)

    return QueryResult(
        query_id=query_id,
        question=question,
        difficulty=difficulty,
        ground_truth_sql=ground_truth_sql.strip(),
        generated_sql=generated_sql.strip(),
        execution_success=True,
        execution_error=None,
        ground_truth_rows=gt_rows,
        generated_rows=gen_rows,
        result_match=match_score,
    )
