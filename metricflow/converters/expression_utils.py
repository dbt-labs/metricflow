from __future__ import annotations

import re
from typing import Dict, Optional, Tuple

from metricflow_semantic_interfaces.type_enums import AggregationType

# Keyed by (dataset_name_or_None, col_name) → (AggregationType, bare_col_expr).
# None as dataset name means the column reference was unqualified in the expression.
_MeasureKey = Tuple[Optional[str], str]

_SIMPLE_AGG_MAP: Dict[str, AggregationType] = {
    "SUM": AggregationType.SUM,
    "COUNT": AggregationType.COUNT,
    "AVG": AggregationType.AVERAGE,
    "MIN": AggregationType.MIN,
    "MAX": AggregationType.MAX,
}


def _strip_qualifier(col: str) -> str:
    """Strip a leading dataset qualifier, e.g. 'orders.amount' → 'amount'."""
    return col.rsplit(".", 1)[-1] if "." in col else col


def _extract_agg_info(expression: str) -> Optional[Tuple[AggregationType, str]]:
    """Parse a simple SQL aggregation expression.

    Returns ``(agg_type, bare_col)`` for recognised patterns, ``None`` otherwise.
    The returned column name has any dataset qualifier stripped.
    """
    expr = expression.strip()

    # COUNT(DISTINCT col)
    m = re.fullmatch(r"COUNT\s*\(\s*DISTINCT\s+(.+?)\s*\)", expr, re.IGNORECASE)
    if m:
        return AggregationType.COUNT_DISTINCT, _strip_qualifier(m.group(1))

    # SUM(CASE WHEN col THEN 1 ELSE 0 END)
    m = re.fullmatch(r"SUM\s*\(\s*CASE\s+WHEN\s+(.+?)\s+THEN\s+1\s+ELSE\s+0\s+END\s*\)", expr, re.IGNORECASE)
    if m:
        return AggregationType.SUM_BOOLEAN, _strip_qualifier(m.group(1))

    # PERCENTILE_CONT/DISC(p) WITHIN GROUP (ORDER BY col)
    m = re.fullmatch(
        r"(PERCENTILE_CONT|PERCENTILE_DISC)\s*\(\s*([0-9.]+)\s*\)\s*WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+(.+?)\s*\)",
        expr,
        re.IGNORECASE,
    )
    if m:
        p = float(m.group(2))
        col = _strip_qualifier(m.group(3))
        if p == 0.5 and m.group(1).upper() == "PERCENTILE_CONT":
            return AggregationType.MEDIAN, col
        return AggregationType.PERCENTILE, col

    # Simple: SUM(col), COUNT(col), AVG(col), MIN(col), MAX(col)
    # Use [^()]+ to reject expressions with nested parentheses (e.g. SUM(a) + SUM(b)).
    m = re.fullmatch(r"([A-Za-z_]+)\s*\(\s*([^()]+?)\s*\)", expr)
    if m:
        func = m.group(1).upper()
        col = _strip_qualifier(m.group(2))
        if func in _SIMPLE_AGG_MAP:
            return _SIMPLE_AGG_MAP[func], col

    return None


def _try_parse_ratio(expr_str: str) -> Optional[Tuple[str, str]]:
    """Try to parse ``(expr_a) / (expr_b)`` returning ``(num_expr, den_expr)`` or None."""
    s = expr_str.strip()
    if not s.startswith("("):
        return None
    depth = 0
    for i, ch in enumerate(s):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                rest = s[i + 1 :].strip()
                if rest.startswith("/"):
                    num_expr = s[1:i].strip()
                    den_part = rest[1:].strip()
                    if den_part.startswith("(") and den_part.endswith(")"):
                        den_expr = den_part[1:-1].strip()
                    else:
                        den_expr = den_part
                    return num_expr, den_expr
                break
    return None


def _get_raw_inner_col(expression: str) -> Optional[str]:
    """Extract the raw column reference from inside a simple aggregation, before stripping qualifiers."""
    expr = expression.strip()
    m = re.fullmatch(r"[A-Za-z_]+\s*\(\s*(.+?)\s*\)", expr)
    if m:
        return m.group(1).strip()
    return None
