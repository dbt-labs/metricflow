#!/bin/bash

set -e
set -x

hatch -v run dev-env:python metricflow-semantics/run_pstats.py
