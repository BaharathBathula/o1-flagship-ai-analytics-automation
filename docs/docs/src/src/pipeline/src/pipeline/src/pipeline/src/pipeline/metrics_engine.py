"""
Metrics Engine Module
---------------------
Computes repeatable analytics metrics from validated datasets.
Designed for automated reporting workflows.
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional
import pandas as pd


@dataclass
class MetricResult:
    name: str
    value: Any
    metadata: Optional[Dict[str, Any]] = None


class MetricsEngine:
    def __init__(self):
        self.results = []

    def compute_row_count(self, df: pd.DataFrame) -> MetricResult:
        return MetricResult(name="row_count", value=int(len(df)))

    def compute_column_count(self, df: pd.DataFrame) -> MetricResult:
        return MetricResult(name="column_count", value=int(df.shape[1]))

    def compute_numeric_summary(self, df: pd.DataFrame) -> MetricResult:
        numeric_df = df.select_dtypes(include="number")
        if numeric_df.empty:
            return MetricResult(
                name="numeric_summary",
                value={},
                metadata={"note": "no numeric columns found"},
            )
        summary = numeric_df.describe().to_dict()
        return MetricResult(name="numeric_summary", value=summary)

    def compute_custom_kpis(
        self, df: pd.DataFrame, kpi_rules: Dict[str, Dict[str, str]]
    ) -> MetricResult:
        """
        Example kpi_rules:
        {
          "total_revenue": {"column": "revenue", "agg": "sum"},
          "avg_order_value": {"column": "order_value", "agg": "mean"}
        }
        """
        output = {}

        for kpi_name, rule in kpi_rules.items():
            column = rule.get("column")
            agg = rule.get("agg")

            if column not in df.columns:
                output[kpi_name] = {"error": f"missing column '{column}'"}
                continue

            if agg == "sum":
                output[kpi_name] = float(df[column].sum())
            elif agg == "mean":
                output[kpi_name] = float(df[column].mean())
            elif agg == "min":
                output[kpi_name] = float(df[column].min())
            elif agg == "max":
                output[kpi_name] = float(df[column].max())
            else:
                output[kpi_name] = {"error": f"unsupported aggregation '{agg}'"}

        return MetricResult(name="custom_kpis", value=output)

    def run(
        self,
        df: pd.DataFrame,
        kpi_rules: Optional[Dict[str, Dict[str, str]]] = None,
    ):
        self.results = []
        self.results.append(self.compute_row_count(df))
        self.results.append(self.compute_column_count(df))
        self.results.append(self.compute_numeric_summary(df))

        if kpi_rules:
            self.results.append(self.compute_custom_kpis(df, kpi_rules))

        return self.results

