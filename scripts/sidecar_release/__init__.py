"""Scripts for building and releasing the MetricFlow sidecar binary. See DI-4871.

Every script in this package is stdlib-only by design, so it can be run directly with a
bare `python3` -- no hatch environment or third-party dependency required -- both in CI and
locally on a contributor's own machine, per DI-4871's "scriptify the release process"
decision. Run them as modules from the repo root, e.g.:

    python3 -m scripts.sidecar_release.cut_adhoc_release --dry-run
"""
