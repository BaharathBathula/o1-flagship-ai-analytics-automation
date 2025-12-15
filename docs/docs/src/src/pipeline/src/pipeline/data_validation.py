"""
Data Validation Module
----------------------
Validates incoming datasets before downstream analytics and AI processing.
Designed for production-oriented analytics automation workflows.
"""

import pandas as pd


class DataValidator:
    def __init__(self, required_columns):
        self.required_columns = required_columns

    def validate_schema(self, df: pd.DataFrame):
        missing_cols = [
            col for col in self.required_columns if col not in df.columns
        ]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

    def validate_nulls(self, df: pd.DataFrame):
        null_summary = df.isnull().sum()
        if null_summary.any():
            raise ValueError(
                f"Null values detected:\n{null_summary[null_summary > 0]}"
            )

    def validate_ranges(self, df: pd.DataFrame, range_rules: dict):
        for column, (min_val, max_val) in range_rules.items():
            if column in df.columns:
                if not df[column].between(min_val, max_val).all():
                    raise ValueError(
                        f"Values out of range in column '{column}'"
                    )

    def run_all_checks(self, df: pd.DataFrame, range_rules: dict = None):
        self.validate_schema(df)
        self.validate_nulls(df)
        if range_rules:
            self.validate_ranges(df, range_rules)
        return True

