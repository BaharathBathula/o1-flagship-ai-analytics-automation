"""
Insight Generator
-----------------
Converts computed metrics into short, executive-ready insights.
Rule-based narrative now; can be upgraded to LLM later.
"""

from typing import List, Dict, Any
from src.pipeline.metrics_engine import MetricResult


class InsightGenerator:
    def generate(self, metrics: List[MetricResult]) -> Dict[str, Any]:
        metric_map = {m.name: m.value for m in metrics}

        row_count = metric_map.get("row_count")
        col_count = metric_map.get("column_count")
        kpis = metric_map.get("custom_kpis", {})

        insights = []

        if row_count is not None and col_count is not None:
            insights.append(
                f"Dataset processed successfully with {row_count} rows and {col_count} columns."
            )

        if isinstance(kpis, dict) and kpis:
            if "total_revenue" in kpis and not isinstance(kpis["total_revenue"], dict):
                insights.append(f"Total revenue for the period is {kpis['total_revenue']:.2f}.")
            if "avg_order_value" in kpis and not isinstance(kpis["avg_order_value"], dict):
                insights.append(f"Average order value is {kpis['avg_order_value']:.2f}.")
            if "max_orders" in kpis and not isinstance(kpis["max_orders"], dict):
                insights.append(f"Peak daily orders reached {int(kpis['max_orders'])}.")

        executive_summary = " ".join(insights) if insights else "Metrics computed successfully."

        return {
            "executive_summary": executive_summary,
            "insights": insights,
            "kpis": kpis,
        }

