#!/usr/bin/env python3
"""
Python compatibility test: Run queries from the Python MetricFlow test suite
through the Rust compiler and report which ones compile successfully.

Usage:
    python3 tests/python_compat_test.py

Requires the Rust CLI to be built:
    cargo build -p mf-cli
"""

import json
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
RUST_CLI = SCRIPT_DIR / "../target/debug/mf"
MANIFEST = SCRIPT_DIR / "fixtures/simple_manifest.json"

# Colors
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"


@dataclass
class TestCase:
    name: str
    source: str  # which Python test file this comes from
    metrics: list[str] = field(default_factory=list)
    group_by: list[str] = field(default_factory=list)
    where: list[str] = field(default_factory=list)
    limit: int | None = None
    # Why we might expect this to fail
    skip_reason: str | None = None


# ──────────────────────────────────────────────────────────────────────
# Test cases extracted from test_query_rendering.py
# ──────────────────────────────────────────────────────────────────────
QUERY_RENDERING_TESTS = [
    # Uses multihop manifest, not simple_manifest
    TestCase(
        name="test_multihop_node",
        source="test_query_rendering.py",
        metrics=["txn_count"],
        group_by=["account_id__customer_id__customer_name"],
        skip_reason="multi-hop join (2 entity links)",
    ),
    TestCase(
        name="test_filter_with_where_constraint_on_join_dim",
        source="test_query_rendering.py",
        metrics=["bookings"],
        group_by=["booking__is_instant"],
        where=["{{ Dimension('listing__country_latest') }} = 'us'"],
    ),
    TestCase(
        name="test_partitioned_join",
        source="test_query_rendering.py",
        metrics=["identity_verifications"],
        group_by=["user__home_state"],
        skip_reason="identity_verifications metric may use partitioned join",
    ),
    TestCase(
        name="test_limit_rows",
        source="test_query_rendering.py",
        metrics=["bookings"],
        group_by=["metric_time__day"],
        limit=1,
    ),
    TestCase(
        name="test_distinct_values",
        source="test_query_rendering.py",
        metrics=[],
        group_by=["listing__country_latest"],
        where=["{{ Dimension('listing__country_latest') }} = 'us'"],
        limit=100,
        skip_reason="dimension-only query (no metrics)",
    ),
    TestCase(
        name="test_local_dimension_using_local_entity",
        source="test_query_rendering.py",
        metrics=["listings"],
        group_by=["listing__country_latest"],
    ),
    TestCase(
        name="test_simple_metric_constraint",
        source="test_query_rendering.py",
        metrics=["lux_booking_value_rate_expr"],
        group_by=["metric_time__day"],
    ),
    TestCase(
        name="test_simple_metric_constraint_with_reused_simple_metric",
        source="test_query_rendering.py",
        metrics=["instant_booking_value_ratio"],
        group_by=["metric_time__day"],
    ),
    TestCase(
        name="test_simple_metric_constraint_with_single_expr_and_alias",
        source="test_query_rendering.py",
        metrics=["double_counted_delayed_bookings"],
        group_by=["metric_time__day"],
    ),
    # SCD tests use different manifest
    TestCase(
        name="test_join_to_scd_dimension",
        source="test_query_rendering.py",
        metrics=["family_bookings"],
        group_by=["metric_time"],
        where=["{{ Dimension('listing__capacity') }} > 2"],
        skip_reason="SCD manifest (not simple_manifest)",
    ),
    TestCase(
        name="test_multiple_metrics_no_dimensions",
        source="test_query_rendering.py",
        metrics=["bookings", "listings"],
        group_by=[],
    ),
    TestCase(
        name="test_metric_with_simple_metrics_from_multiple_sources_no_dimensions",
        source="test_query_rendering.py",
        metrics=["bookings_per_listing"],
        group_by=[],
    ),
    TestCase(
        name="test_common_semantic_model",
        source="test_query_rendering.py",
        metrics=["bookings", "booking_value"],
        group_by=["metric_time__day"],
    ),
    TestCase(
        name="test_min_max_only_categorical",
        source="test_query_rendering.py",
        metrics=[],
        group_by=["listing__country_latest"],
        skip_reason="min_max_only query (not supported)",
    ),
    TestCase(
        name="test_non_additive_dimension_with_non_default_grain",
        source="test_query_rendering.py",
        metrics=["total_account_balance_first_day_of_month"],
        group_by=[],
        skip_reason="non-additive dimension / semi-additive measure",
    ),
    TestCase(
        name="test_semi_additive_measure_with_where_filter",
        source="test_query_rendering.py",
        metrics=["current_account_balance_by_user"],
        group_by=["user"],
        where=["{{ Dimension('account__account_type') }} = 'savings'"],
        skip_reason="semi-additive measure",
    ),
]

# ──────────────────────────────────────────────────────────────────────
# Test cases from test_derived_metric_rendering.py
# ──────────────────────────────────────────────────────────────────────
DERIVED_METRIC_TESTS = [
    TestCase(
        name="test_derived_metric",
        source="test_derived_metric_rendering.py",
        metrics=["booking_fees"],
        group_by=["metric_time__day"],
    ),
    TestCase(
        name="test_nested_derived_metric",
        source="test_derived_metric_rendering.py",
        metrics=["views_times_booking_value"],
        group_by=["metric_time__day"],
    ),
    TestCase(
        name="test_derived_metric_with_offset_window",
        source="test_derived_metric_rendering.py",
        metrics=["bookings_growth_2_weeks"],
        group_by=["metric_time__day"],
        skip_reason="offset_window not supported",
    ),
    TestCase(
        name="test_derived_metric_with_offset_to_grain",
        source="test_derived_metric_rendering.py",
        metrics=["bookings_growth_since_start_of_month"],
        group_by=["metric_time__day"],
        skip_reason="offset_to_grain not supported",
    ),
    TestCase(
        name="test_derived_metric_with_one_input_metric",
        source="test_derived_metric_rendering.py",
        metrics=["booking_fees_per_booker"],
        group_by=["metric_time__day"],
    ),
    TestCase(
        name="test_derived_metric_that_defines_the_same_alias_in_different_components",
        source="test_derived_metric_rendering.py",
        metrics=["derived_bookings_0"],
        group_by=["metric_time__day"],
    ),
    TestCase(
        name="test_derived_metric_with_joined_where_constraint_not_on_entity",
        source="test_derived_metric_rendering.py",
        metrics=["booking_fees"],
        group_by=["metric_time__day"],
        where=["{{ Dimension('listing__country_latest') }} = 'us'"],
    ),
]

# ──────────────────────────────────────────────────────────────────────
# Test cases from test_cumulative_metric_rendering.py
# ──────────────────────────────────────────────────────────────────────
CUMULATIVE_METRIC_TESTS = [
    TestCase(
        name="test_cumulative_metric",
        source="test_cumulative_metric_rendering.py",
        metrics=["trailing_2_months_revenue"],
        group_by=["metric_time__day"],
    ),
    TestCase(
        name="test_cumulative_metric_no_ds",
        source="test_cumulative_metric_rendering.py",
        metrics=["trailing_2_months_revenue"],
        group_by=[],
    ),
    TestCase(
        name="test_cumulative_metric_no_window",
        source="test_cumulative_metric_rendering.py",
        metrics=["revenue_all_time"],
        group_by=["metric_time__day"],
    ),
    TestCase(
        name="test_cumulative_metric_no_window_no_ds",
        source="test_cumulative_metric_rendering.py",
        metrics=["revenue_all_time"],
        group_by=[],
    ),
    TestCase(
        name="test_cumulative_metric_grain_to_date",
        source="test_cumulative_metric_rendering.py",
        metrics=["revenue_mtd"],
        group_by=["metric_time__day"],
    ),
    TestCase(
        name="test_cumulative_metric_month",
        source="test_cumulative_metric_rendering.py",
        metrics=["trailing_2_months_revenue"],
        group_by=["metric_time__month"],
    ),
    TestCase(
        name="test_cumulative_metric_with_non_default_grain",
        source="test_cumulative_metric_rendering.py",
        metrics=["trailing_2_months_revenue"],
        group_by=["metric_time__week"],
    ),
    TestCase(
        name="test_cumulative_metric_with_agg_time_dimension",
        source="test_cumulative_metric_rendering.py",
        metrics=["trailing_2_months_revenue"],
        group_by=["revenue_instance__ds__day"],
        skip_reason="agg_time_dimension via entity path",
    ),
]

# ──────────────────────────────────────────────────────────────────────
# Test cases from test_fill_nulls_with_rendering.py
# ──────────────────────────────────────────────────────────────────────
FILL_NULLS_TESTS = [
    TestCase(
        name="test_simple_fill_nulls_with_0_metric_time",
        source="test_fill_nulls_with_rendering.py",
        metrics=["bookings_fill_nulls_with_0"],
        group_by=["metric_time__day"],
        skip_reason="fill_nulls requires time spine join",
    ),
    TestCase(
        name="test_simple_fill_nulls_without_time_spine",
        source="test_fill_nulls_with_rendering.py",
        metrics=["bookings_fill_nulls_with_0_without_time_spine"],
        group_by=["metric_time__day"],
    ),
    TestCase(
        name="test_simple_fill_nulls_with_0_no_groupby",
        source="test_fill_nulls_with_rendering.py",
        metrics=["bookings_fill_nulls_with_0_without_time_spine"],
        group_by=[],
    ),
]

# ──────────────────────────────────────────────────────────────────────
# Simple metric tests (core functionality)
# ──────────────────────────────────────────────────────────────────────
SIMPLE_METRIC_TESTS = [
    TestCase(
        name="test_simple_metric_no_groupby",
        source="synthetic",
        metrics=["bookings"],
        group_by=[],
    ),
    TestCase(
        name="test_simple_metric_with_metric_time",
        source="synthetic",
        metrics=["bookings"],
        group_by=["metric_time__day"],
    ),
    TestCase(
        name="test_simple_metric_with_metric_time_month",
        source="synthetic",
        metrics=["bookings"],
        group_by=["metric_time__month"],
    ),
    TestCase(
        name="test_simple_metric_with_dimension",
        source="synthetic",
        metrics=["bookings"],
        group_by=["booking__is_instant"],
    ),
    TestCase(
        name="test_simple_metric_with_joined_dimension",
        source="synthetic",
        metrics=["bookings"],
        group_by=["listing__country_latest"],
    ),
    TestCase(
        name="test_simple_metric_with_time_and_dimension",
        source="synthetic",
        metrics=["bookings"],
        group_by=["metric_time__day", "booking__is_instant"],
    ),
    TestCase(
        name="test_simple_metric_booking_value",
        source="synthetic",
        metrics=["booking_value"],
        group_by=["metric_time__day"],
    ),
    TestCase(
        name="test_simple_metric_max_booking_value",
        source="synthetic",
        metrics=["max_booking_value"],
        group_by=["metric_time__day"],
    ),
    TestCase(
        name="test_simple_metric_min_booking_value",
        source="synthetic",
        metrics=["min_booking_value"],
        group_by=["metric_time__day"],
    ),
    TestCase(
        name="test_simple_metric_bookers",
        source="synthetic",
        metrics=["bookers"],
        group_by=["metric_time__day"],
    ),
    TestCase(
        name="test_simple_metric_views",
        source="synthetic",
        metrics=["views"],
        group_by=["metric_time__day"],
    ),
    TestCase(
        name="test_simple_metric_listings",
        source="synthetic",
        metrics=["listings"],
        group_by=["metric_time__day"],
    ),
    TestCase(
        name="test_simple_metric_with_where",
        source="synthetic",
        metrics=["bookings"],
        group_by=["metric_time__day"],
        where=["{{ Dimension('booking__is_instant') }} = true"],
    ),
    TestCase(
        name="test_simple_metric_instant_bookings",
        source="synthetic",
        metrics=["instant_bookings"],
        group_by=["metric_time__day"],
    ),
    TestCase(
        name="test_simple_metric_referred_bookings",
        source="synthetic",
        metrics=["referred_bookings"],
        group_by=["metric_time__day"],
    ),
]

# ──────────────────────────────────────────────────────────────────────
# Ratio metric tests
# ──────────────────────────────────────────────────────────────────────
RATIO_METRIC_TESTS = [
    TestCase(
        name="test_ratio_bookings_per_listing",
        source="synthetic",
        metrics=["bookings_per_listing"],
        group_by=["metric_time__day"],
    ),
    TestCase(
        name="test_ratio_bookings_per_view",
        source="synthetic",
        metrics=["bookings_per_view"],
        group_by=["metric_time__day"],
    ),
    TestCase(
        name="test_ratio_bookings_per_dollar",
        source="synthetic",
        metrics=["bookings_per_dollar"],
        group_by=["metric_time__day"],
    ),
]

# ──────────────────────────────────────────────────────────────────────
# Multi-metric tests
# ──────────────────────────────────────────────────────────────────────
MULTI_METRIC_TESTS = [
    TestCase(
        name="test_multi_metric_bookings_and_listings",
        source="synthetic",
        metrics=["bookings", "listings"],
        group_by=["metric_time__day"],
    ),
    TestCase(
        name="test_multi_metric_bookings_and_booking_value",
        source="synthetic",
        metrics=["bookings", "booking_value"],
        group_by=["metric_time__day"],
    ),
    TestCase(
        name="test_multi_metric_three_metrics",
        source="synthetic",
        metrics=["bookings", "listings", "views"],
        group_by=["metric_time__day"],
    ),
]


ALL_TESTS = (
    SIMPLE_METRIC_TESTS
    + QUERY_RENDERING_TESTS
    + DERIVED_METRIC_TESTS
    + CUMULATIVE_METRIC_TESTS
    + FILL_NULLS_TESTS
    + RATIO_METRIC_TESTS
    + MULTI_METRIC_TESTS
)


def run_test(tc: TestCase) -> tuple[str, str | None]:
    """Run a test case. Returns ("pass"|"fail"|"skip", error_message_or_None)."""
    if tc.skip_reason:
        return "skip", tc.skip_reason

    if not tc.metrics:
        return "skip", "no metrics (dimension-only query)"

    cmd = [
        str(RUST_CLI),
        "query",
        "--manifest", str(MANIFEST),
        "--metrics", ",".join(tc.metrics),
        "--dialect", "duckdb",
    ]

    if tc.group_by:
        cmd.extend(["--group-by", ",".join(tc.group_by)])

    for w in tc.where:
        cmd.extend(["--where", w])

    if tc.limit is not None:
        cmd.extend(["--limit", str(tc.limit)])

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            # Verify it actually produced SQL
            if "SELECT" in result.stdout.upper():
                return "pass", None
            else:
                return "fail", f"No SQL output: {result.stdout[:200]}"
        else:
            return "fail", result.stderr.strip()[:200]
    except subprocess.TimeoutExpired:
        return "fail", "timeout"
    except Exception as e:
        return "fail", str(e)[:200]


def main():
    # Build the CLI first
    print(f"{BOLD}Building Rust CLI...{RESET}")
    build = subprocess.run(
        ["cargo", "build", "-p", "mf-cli"],
        capture_output=True, text=True, cwd=SCRIPT_DIR / "..",
    )
    if build.returncode != 0:
        print(f"{RED}Build failed:{RESET}\n{build.stderr}")
        sys.exit(1)

    print(f"\n{BOLD}Running {len(ALL_TESTS)} test cases from Python MetricFlow test suite{RESET}")
    print(f"Manifest: {MANIFEST}\n")

    pass_count = 0
    fail_count = 0
    skip_count = 0
    failures = []

    # Group by source file
    sources = {}
    for tc in ALL_TESTS:
        sources.setdefault(tc.source, []).append(tc)

    for source, tests in sources.items():
        print(f"\n{BOLD}{CYAN}── {source} ({len(tests)} tests) ──{RESET}")
        for tc in tests:
            status, msg = run_test(tc)
            if status == "pass":
                pass_count += 1
                print(f"  {GREEN}PASS{RESET} {tc.name}")
            elif status == "skip":
                skip_count += 1
                print(f"  {YELLOW}SKIP{RESET} {tc.name} — {msg}")
            else:
                fail_count += 1
                failures.append((tc, msg))
                print(f"  {RED}FAIL{RESET} {tc.name}")
                print(f"       {msg}")

    # Summary
    total = pass_count + fail_count + skip_count
    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}Results: {GREEN}{pass_count} passed{RESET}, {RED}{fail_count} failed{RESET}, {YELLOW}{skip_count} skipped{RESET} / {total} total")

    compiles_pct = pass_count / (pass_count + fail_count) * 100 if (pass_count + fail_count) > 0 else 0
    print(f"{BOLD}Compile rate: {compiles_pct:.0f}% ({pass_count}/{pass_count + fail_count} non-skipped){RESET}")

    if failures:
        print(f"\n{BOLD}{RED}Failures:{RESET}")
        for tc, msg in failures:
            metrics_str = ",".join(tc.metrics) if tc.metrics else "(none)"
            group_str = ",".join(tc.group_by) if tc.group_by else "(none)"
            print(f"  {tc.name}: metrics={metrics_str} group_by={group_str}")
            print(f"    {msg}")

    sys.exit(1 if fail_count > 0 else 0)


if __name__ == "__main__":
    main()
