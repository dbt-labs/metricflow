from __future__ import annotations

import argparse
import json
from io import StringIO

from metricflow_semantics.test_helpers.performance.profiling import (
    PerformanceMetricComparison,
    SessionReportSet,
    SessionReportSetComparison,
)

REPORT_TOP = 10

NEGLIGIBLE_DELTA = 10e-7

WARNING_PERCENT_THRESHOLD = 0.1

LEGEND_HEADER = f"""
## Legend
- **Function**: the function name
- **Base calls**: number of calls, excluding recursion
- **Total calls**: number of calls, including recursion
- **Body time**: time spent on the function body, excluding sub-function calls
- **Total time**: time spent on the function body and in other sub-functions it calls

`n.d.` means "negligible difference", i.e the difference between measured outputs is negligible, and any differences could be down to imprecision, so the reported value is only the absolute measured value

Warnings ( :warning: ) will be emitted whenever there is more than {WARNING_PERCENT_THRESHOLD*100:.0f}% increase in total runtime of a test.
""".strip()


def _load_report_file(filename: str) -> SessionReportSet:
    with open(filename, "r") as f:
        raw = f.read()
    return SessionReportSet.parse_obj(json.loads(raw))


def _is_negligible(val: int | float) -> bool:
    return abs(val) < NEGLIGIBLE_DELTA


def _format_num(num: int | float) -> str:
    return f"{num:+.3f}" if isinstance(num, float) else str(num)


def _format_comp(comp: PerformanceMetricComparison[int] | PerformanceMetricComparison[float], unit: str = "") -> str:
    """Return a float percent value as (val_as_percent%)."""
    a_fmt = _format_num(comp.a)
    b_fmt = _format_num(comp.b)
    abs_fmt = _format_num(comp.abs)

    if _is_negligible(comp.abs):
        return f"{a_fmt}{unit} (n.d.)"

    return f"{a_fmt}{unit} - {b_fmt}{unit} = {abs_fmt}{unit} ({comp.pct * 100:+.3f}%)"


# I hate this code but there's no real elegant way of creating a markdown file
def _report_comparison_markdown(
    base_name: str, other_name: str, comp: SessionReportSetComparison, report_top: int
) -> str:
    buf = StringIO()

    buf.write("# Performance comparison\n")
    buf.write(f"Comparing `{base_name}` against `{other_name}`\n\n")
    buf.write(
        f"Reporting top {report_top} highest non-negligible absolute total time differences for each session.\n\n"
    )

    buf.write(LEGEND_HEADER + "\n\n")
    buf.write("-------------\n\n")

    for session, session_comp in comp.sessions.items():
        warn_str = (
            " :warning: :warning:"
            if session_comp is not None and session_comp.total_time.pct > WARNING_PERCENT_THRESHOLD
            else ""
        )

        buf.write(f"### `{session}`{warn_str}\n\n")
        if session_comp is None:
            buf.write("Comparison not available since there's no data for this session in one of the reports.\n\n")
            continue

        total_time_fmt = _format_comp(session_comp.total_time, "s")
        buf.write(f"**Total time:** {total_time_fmt}\n\n")

        buf.write(
            "| i | Function | Base calls | Total calls | Body time (cumulative) | Body time (per call) | Total time (cumulative) | Total time (per call) |\n"
        )
        buf.write(
            "| - | -------- | ---------- | ----------- | ---------------------- | -------------------- | ----------------------- | --------------------- |\n"
        )

        top_functions = sorted(
            (
                (func_name, abs(func_comp.total_time.abs))
                for func_name, func_comp in session_comp.functions.items()
                if func_comp is not None and not _is_negligible(func_comp.total_time.abs)
            ),
            key=lambda tup: tup[1],
            reverse=True,
        )

        for i, (func_name, _) in enumerate(top_functions[0:report_top]):
            buf.write(f"| #{i+1} | `{func_name}` ")

            func_comp = session_comp.functions[func_name]

            if func_comp is None:
                buf.write(" | n/a" * 6)
            else:
                buf.write("| ")
                buf.write(
                    " | ".join(
                        [
                            _format_comp(func_comp.base_calls),
                            _format_comp(func_comp.total_calls),
                            _format_comp(func_comp.body_time, "s"),
                            _format_comp(func_comp.per_call_body_time, "s"),
                            _format_comp(func_comp.total_time, "s"),
                            _format_comp(func_comp.per_call_total_time, "s"),
                        ]
                    )
                )

            buf.write(" |\n")
        buf.write("\n\n")

    return buf.getvalue().strip()


def main() -> None:  # noqa: D103
    parser = argparse.ArgumentParser()
    parser.add_argument("a", help="The base report for the comparison")
    parser.add_argument("b", help="The other report for the comparison")
    parser.add_argument("output", help="The output file for the comparison")
    parser.add_argument(
        "--top", type=int, default=REPORT_TOP, help="Number of functions to report, sorted by percent difference"
    )

    args = parser.parse_args()

    a = _load_report_file(args.a)
    b = _load_report_file(args.b)

    comparison = a.compare(b)
    md = _report_comparison_markdown(
        base_name=args.a,
        other_name=args.b,
        comp=comparison,
        report_top=args.top,
    )
    with open(args.output, "w") as f:
        f.write(md)

    print(args.output)


if __name__ == "__main__":
    main()
