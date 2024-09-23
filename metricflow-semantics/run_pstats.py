from __future__ import annotations

import pstats

CPROFILE_OUTPUT_FILE_NAME = "cprofile_output.bin"

if __name__ == "__main__":
    p = pstats.Stats(CPROFILE_OUTPUT_FILE_NAME)
    p.strip_dirs()
    p.sort_stats("cumtime")
    p.print_stats(50)
