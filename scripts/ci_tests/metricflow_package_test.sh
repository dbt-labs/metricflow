#!/bin/bash
# Tests that the `metricflow` package works properly.
# TODO: Consolidate the boiler plate.

# Root directory where `hatch build`, relative to the repo root.
RELATIVE_PACKAGE_DIR="."

# Show each command and fail the script if any command fails.
set -e
set -x

# Switch to the repo directory.
SCRIPT_DIR=`dirname "$0"`
echo "SCRIPT_DIR=$SCRIPT_DIR"
REAL_SCRIPT_DIR=`REAL_SCRIPT_DIR=$REAL_SCRIPT_DIR`
REAL_REPO_DIR=`realpath "$SCRIPT_DIR/../.."`
echo "REAL_REPO_DIR=$REAL_REPO_DIR"
cd "$REAL_REPO_DIR"

# Build the wheels.
PACKAGE_DIR="$REAL_REPO_DIR/$RELATIVE_PACKAGE_DIR"
cd "$PACKAGE_DIR"
hatch clean
hatch build

# Create a temporary directory and switch to it.
TEMP_DIR=`mktemp -d`
echo "TEMP_DIR=$TEMP_DIR"
cd "$TEMP_DIR"

# Create a Python virtual env.
python3 -m venv venv
source venv/bin/activate

# Install the wheel.
pip3 install "$PACKAGE_DIR/dist/"*.whl

# Run the test
python3 "$REAL_REPO_DIR/scripts/ci_tests/metricflow_package_test.py"
