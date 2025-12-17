"""
End-to-End Orchestration Script
-------------------------------
Runs the flagship AI automation workflow:
1) Load sample data
2) Validate data
3) Compute metrics/KPIs
4) Export report + write audit log
"""

import pandas as pd

from src.pipeline.data_validation import DataValidator
from src.pipeline.metrics_engine import MetricsEngine
from src.delivery.report_delivery import ReportDelivery


def build_sample_dataset() -> pd.DataFrame:
    # Sample dataset to demonstrate workflow (replace with real ingestion later)
    return pd.DataFrame(
        {
            "date": ["2025-12-01", "2025-12-02", "2025-12-03"],
            "revenue": [1200.0, 980.5, 1500.25],
            "orders": [40, 32, 55],
            "order_value": [30.0, 30.64, 27.28],
        }
    )


def run_workflow():
    print("Starting workflow...")

    # 1) Load data
    df = build_sample_dataset()
    print(f"Loaded dataset with shape: {df.shape}")

    # 2) Validate
    validator = DataValidator(required_columns=["date", "revenue", "orders", "order_value"])
    validator.run_all_checks(
        df,
        range_rules={
            "revenue": (0, 1_000_000),
            "orders": (0, 1_000_000),
            "order_value": (0, 1_000_000),
        },
    )
    print("Validation passed.")

    # 3) Compute metrics + KPIs
    kpi_rules = {
        "total_revenue": {"column": "revenue", "agg": "sum"},
        "avg_order_value": {"column": "order_value", "agg": "mean"},
        "max_orders": {"column": "orders", "agg": "max"},
    }
    engine = MetricsEngine()
    results = engine.run(df, kpi_rules=kpi_rules)
    print("Metrics computed.")

    # 4) Deliver report + audit log
    delivery = ReportDelivery(output_dir="outputs", log_dir="logs")
    report_path = delivery.export_metrics_to_json(results, filename="report_metrics.json")
    log_path = delivery.log_delivery_event(report_path=report_path)

    print(f"Report exported to: {report_path}")
    print(f"Delivery log updated: {log_path}")
    print("Workflow complete.")


if __name__ == "__main__":
    run_workflow()

