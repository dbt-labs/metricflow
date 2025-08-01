from __future__ import annotations

import pstats
from pathlib import Path

CPROFILE_OUTPUT_FILE_PATH = Path.cwd().joinpath("git_ignored/cprofile_output.bin")

if __name__ == "__main__":
    CPROFILE_OUTPUT_FILE_PATH.parent.mkdir(exist_ok=True)
    p = pstats.Stats(str(CPROFILE_OUTPUT_FILE_PATH))
    p.strip_dirs()
    p.sort_stats("cumtime")
    p.print_stats(50)
    print(f"Read from {str(CPROFILE_OUTPUT_FILE_PATH)}")
