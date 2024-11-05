from __future__ import annotations

import argparse
import json
from io import StringIO

from metricflow_semantics.test_helpers.performance_helpers import (
    SessionReportSet,
    SessionReportSetComparison,
)

MAX_PCT_CHANGE_WARNING_THRESHOLD = 0.15


def _load_report_file(filename: str) -> SessionReportSet:
    with open(filename, "r") as f:
        raw = f.read()
    return SessionReportSet.parse_obj(json.loads(raw))


# I hate this code but there's no real elegant way of creating a markdown file
def _report_comparison_markdown(base_name: str, other_name: str, comp: SessionReportSetComparison) -> str:
    buf = StringIO()

    buf.write("# Performance comparison\n")
    buf.write(f"Comparing `{base_name}` against `{other_name}`\n\n")
    buf.write(f"**Worst performance hit:** {comp.max_pct_change * 100:.2f}% in `{comp.max_pct_change_session}`\n\n")

    for session, session_comp in comp.sessions.items():
        emoji = (
            ":question:"
            if session_comp is None
            else (":bangbang:" if session_comp.max_pct_change > MAX_PCT_CHANGE_WARNING_THRESHOLD else ":rocket:")
        )

        buf.write(f"## `{session}` {emoji}\n\n")
        if session_comp is None:
            buf.write("Comparison not available since there's no data for this session in one of the reports.\n\n")
            continue

        buf.write("| context | CPU avg | CPU median | CPU max | Wall avg | Wall median | Wall max |\n")
        buf.write("| ------- | ------- | ---------- | ------- | -------- | ----------- | -------- |\n")
        for ctx, ctx_comp in session_comp.contexts.items():
            buf.write(f"| `{ctx}` ")

            if ctx_comp is None:
                buf.write(" | n/a" * 6)
            else:
                buf.write("| ")
                buf.write(
                    " | ".join(
                        f"{int(abs)/10e6:.4f}ms ({pct * 100:+.2f}%)"
                        for abs, pct in (
                            (ctx_comp.cpu_ns_average_abs, ctx_comp.cpu_ns_average_pct),
                            (ctx_comp.cpu_ns_median_abs, ctx_comp.cpu_ns_median_pct),
                            (ctx_comp.cpu_ns_max_abs, ctx_comp.cpu_ns_max_pct),
                            (ctx_comp.wall_ns_average_abs, ctx_comp.wall_ns_average_pct),
                            (ctx_comp.wall_ns_median_abs, ctx_comp.wall_ns_median_pct),
                            (ctx_comp.wall_ns_max_abs, ctx_comp.wall_ns_max_pct),
                        )
                    )
                )

            buf.write(" |\n")
        buf.write("\n\n")

    return buf.getvalue()


def main() -> None:  # noqa: D103
    parser = argparse.ArgumentParser()
    parser.add_argument("a", help="The base report for the comparison")
    parser.add_argument("b", help="The other report for the comparison")
    parser.add_argument("output", help="The output file for the comparison")

    args = parser.parse_args()

    a = _load_report_file(args.a)
    b = _load_report_file(args.b)

    comparison = a.compare(b)
    md = _report_comparison_markdown(
        base_name=args.a,
        other_name=args.b,
        comp=comparison,
    )
    with open(args.output, "w") as f:
        f.write(md)

    print(args.output)


if __name__ == "__main__":
    main()
