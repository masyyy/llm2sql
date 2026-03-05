"""Pydantic models for benchmark results."""

from pydantic import BaseModel


class QueryResult(BaseModel):
    query_id: int
    question: str
    difficulty: str
    ground_truth_sql: str
    generated_sql: str
    execution_success: bool
    execution_error: str | None = None
    ground_truth_rows: list[list] = []
    generated_rows: list[list] = []
    result_match: float  # 0, 0.5, or 1


class BenchmarkResult(BaseModel):
    model_name: str
    timestamp: str
    total_queries: int
    execution_accuracy: float
    result_match_avg: float
    results: list[QueryResult]
