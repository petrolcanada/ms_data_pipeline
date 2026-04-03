"""
Data Validation Hooks
Optional per-column checks that can be configured in tables.yaml.

Example tables.yaml entry:
    validation:
      _ID:
        not_null: true
      MONTHENDDATE:
        not_null: true
        min_value: "2000-01-01"
"""
import pandas as pd
from typing import Dict, Any, List, Optional
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


def validate_dataframe(
    df: pd.DataFrame,
    validation_rules: Optional[Dict[str, Dict[str, Any]]] = None,
    table_name: str = "",
) -> List[Dict[str, Any]]:
    """Run validation checks on a DataFrame and return a list of findings.

    Each finding is a dict with keys: column, check, passed, detail.
    """
    if not validation_rules:
        return []

    findings: List[Dict[str, Any]] = []

    for col_name, rules in validation_rules.items():
        col_lower = col_name.lower()
        matched_col = None
        for c in df.columns:
            if c.lower() == col_lower:
                matched_col = c
                break
        if matched_col is None:
            findings.append({
                "column": col_name,
                "check": "column_exists",
                "passed": False,
                "detail": f"Column {col_name} not found in DataFrame",
            })
            continue

        series = df[matched_col]

        if rules.get("not_null"):
            null_count = int(series.isnull().sum())
            passed = null_count == 0
            findings.append({
                "column": col_name,
                "check": "not_null",
                "passed": passed,
                "detail": f"{null_count} null(s)" if not passed else "OK",
            })

        if "null_rate_max" in rules:
            threshold = float(rules["null_rate_max"])
            null_rate = float(series.isnull().mean())
            passed = null_rate <= threshold
            findings.append({
                "column": col_name,
                "check": "null_rate_max",
                "passed": passed,
                "detail": f"null_rate={null_rate:.4f} (max {threshold})",
            })

        if "min_value" in rules:
            min_val = rules["min_value"]
            try:
                actual_min = series.dropna().min()
                passed = str(actual_min) >= str(min_val)
                findings.append({
                    "column": col_name,
                    "check": "min_value",
                    "passed": passed,
                    "detail": f"min={actual_min} (expected >= {min_val})",
                })
            except Exception as e:
                findings.append({
                    "column": col_name,
                    "check": "min_value",
                    "passed": False,
                    "detail": str(e),
                })

    failed = [f for f in findings if not f["passed"]]
    if failed:
        logger.warning(f"Validation issues for {table_name}: {len(failed)} check(s) failed")
        for f in failed:
            logger.warning(f"  {f['column']}.{f['check']}: {f['detail']}")

    return findings
