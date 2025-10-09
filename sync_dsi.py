#!/usr/bin/env python3
"""Script to sync dbt-semantic-interfaces files to metricflow-semantic-interfaces folder.

This script assumes that dbt-semantic-interfaces is cloned in the same parent directory
as this metricflow repository.

The script will:
1. Pull the latest main branch from dbt-semantic-interfaces repository
2. Copy the following:
   - dbt-semantic-interfaces/dbt_semantic_interfaces/ contents to metricflow_semantic_interfaces/
   - dbt-semantic-interfaces/tests/ contents to tests_metricflow/mf_semantic_interfaces/
   - dbt-semantic-interfaces/dsi_pydantic_shim.py to metricflow_semantic_interfaces/
3. Replace all dbt_semantic_interfaces imports in the repository with metricflow_semantic_interfaces
"""
from __future__ import annotations

import re
import shutil
import subprocess
import sys
from pathlib import Path


def run_git_command(command, cwd) -> str:
    """Run a git command in the specified directory."""
    try:
        result = subprocess.run(command, cwd=cwd, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Git command failed: {' '.join(command)}")
        print(f"Error output: {e.stderr}")
        sys.exit(1)


def update_dsi_repository(dsi_repo_path) -> None:
    """Update the dbt-semantic-interfaces repository to the latest main branch."""
    print(f"Updating dbt-semantic-interfaces repository at {dsi_repo_path}...")

    # Check if it's a git repository
    git_dir = dsi_repo_path / ".git"
    if not git_dir.exists():
        print(f"ERROR: {dsi_repo_path} is not a git repository")
        sys.exit(1)

    # Get current branch
    current_branch = run_git_command(["git", "branch", "--show-current"], dsi_repo_path)
    print(f"Current branch: {current_branch}")

    # Check if there are uncommitted changes
    status_output = run_git_command(["git", "status", "--porcelain"], dsi_repo_path)
    if status_output:
        print("WARNING: There are uncommitted changes in the dbt-semantic-interfaces repository:")
        print(status_output)
        response = input("Continue anyway? (y/N): ").strip().lower()
        if response != "y":
            print("Aborting sync.")
            sys.exit(1)

    # Switch to main branch if not already on it
    if current_branch != "main":
        print(f"Switching from {current_branch} to main branch...")
        run_git_command(["git", "checkout", "main"], dsi_repo_path)

    # Pull latest changes
    print("Pulling latest changes from origin/main...")
    run_git_command(["git", "pull", "origin", "main"], dsi_repo_path)

    print("✅ Successfully updated dbt-semantic-interfaces repository")


def replace_dsi_imports(repo_root) -> None:
    """Replace all dbt_semantic_interfaces imports with metricflow_semantic_interfaces."""
    print("Replacing dbt_semantic_interfaces imports in the repository...")

    # Find all Python files that might contain imports
    python_files = []
    for pattern in ["**/*.py"]:
        python_files.extend(repo_root.glob(pattern))

    # Patterns to replace
    import_patterns = [
        # from metricflow_semantic_interfaces.* import ...
        (r"from dbt_semantic_interfaces\.", "from metricflow_semantic_interfaces."),
        # import metricflow_semantic_interfaces.*
        (r"import dbt_semantic_interfaces\.", "import metricflow_semantic_interfaces."),
        # metricflow_semantic_interfaces.* (usage in code)
        (r"\bdbt_semantic_interfaces\.", "metricflow_semantic_interfaces."),
    ]

    files_modified = 0
    total_replacements = 0

    for file_path in python_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            original_content = content
            file_replacements = 0

            for pattern, replacement in import_patterns:
                new_content, count = re.subn(pattern, replacement, content)
                content = new_content
                file_replacements += count

            if content != original_content:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(content)
                files_modified += 1
                total_replacements += file_replacements
                print(f"  Updated {file_path.relative_to(repo_root)} ({file_replacements} replacements)")

        except Exception as e:
            print(f"  WARNING: Could not process {file_path}: {e}")

    print(f"✅ Import replacement complete: {total_replacements} replacements in {files_modified} files")


def main() -> None:
    """Main function to copy dbt-semantic-interfaces files."""
    # Get the current script directory (metricflow repo root)
    script_dir = Path(__file__).parent.absolute()

    # Assume dbt-semantic-interfaces is in the same parent directory
    parent_dir = script_dir.parent
    dsi_repo_path = parent_dir / "dbt-semantic-interfaces"

    # Target directories in this repo
    target_dir = script_dir / "metricflow_semantic_interfaces"
    tests_target_dir = script_dir / "tests_metricflow" / "mf_semantic_interfaces"

    print(f"Script directory: {script_dir}")
    print(f"Looking for dbt-semantic-interfaces at: {dsi_repo_path}")
    print(f"Target directory for code: {target_dir}")
    print(f"Target directory for tests: {tests_target_dir}")

    # Validate that dbt-semantic-interfaces exists
    if not dsi_repo_path.exists():
        print(f"ERROR: dbt-semantic-interfaces repository not found at {dsi_repo_path}")
        print("Please ensure dbt-semantic-interfaces is cloned in the same parent directory as this repo.")
        sys.exit(1)

    # Update dbt-semantic-interfaces to latest main
    update_dsi_repository(dsi_repo_path)

    # Define source paths
    source_dbt_semantic_interfaces = dsi_repo_path / "dbt_semantic_interfaces"
    source_tests = dsi_repo_path / "tests"
    source_pydantic_shim = dsi_repo_path / "dsi_pydantic_shim.py"

    # Validate source paths exist
    missing_sources = []
    if not source_dbt_semantic_interfaces.exists():
        missing_sources.append(str(source_dbt_semantic_interfaces))
    if not source_tests.exists():
        missing_sources.append(str(source_tests))
    if not source_pydantic_shim.exists():
        missing_sources.append(str(source_pydantic_shim))

    if missing_sources:
        print("ERROR: The following expected files/folders were not found in dbt-semantic-interfaces:")
        for missing in missing_sources:
            print(f"  - {missing}")
        sys.exit(1)

    # Create target directories if they don't exist
    if target_dir.exists():
        print(f"Target directory {target_dir} already exists. Contents will be replaced.")
        shutil.rmtree(target_dir)

    if tests_target_dir.exists():
        print(f"Tests target directory {tests_target_dir} already exists. Contents will be replaced.")
        shutil.rmtree(tests_target_dir)

    target_dir.mkdir(parents=True, exist_ok=True)
    tests_target_dir.mkdir(parents=True, exist_ok=True)
    print(f"Created target directories: {target_dir} and {tests_target_dir}")

    # Copy the folders and file
    try:
        print("Copying dbt_semantic_interfaces contents to metricflow_semantic_interfaces...")
        # Copy contents of dbt_semantic_interfaces directly to target_dir
        for item in source_dbt_semantic_interfaces.iterdir():
            if item.is_dir():
                shutil.copytree(item, target_dir / item.name)
            else:
                shutil.copy2(item, target_dir / item.name)

        print("Copying tests contents to tests_metricflow/mf_semantic_interfaces...")
        # Copy contents of tests to tests_target_dir
        for item in source_tests.iterdir():
            if item.is_dir():
                shutil.copytree(item, tests_target_dir / item.name)
            else:
                shutil.copy2(item, tests_target_dir / item.name)

        print("Copying dsi_pydantic_shim.py file...")
        shutil.copy2(source_pydantic_shim, target_dir / "dsi_pydantic_shim.py")

        # Create __init__.py files to make it a proper Python package
        print("Creating __init__.py files...")
        (target_dir / "__init__.py").touch()
        (tests_target_dir / "__init__.py").touch()

        print("✅ Successfully copied all files!")
        print(f"Code files copied to: {target_dir}")
        print(f"Test files copied to: {tests_target_dir}")

        # List what was copied
        print(f"\nCopied contents to {target_dir.name}:")
        for item in sorted(target_dir.iterdir()):
            if item.is_dir():
                print(f"  📁 {item.name}/")
            else:
                print(f"  📄 {item.name}")

        print(f"\nCopied contents to {tests_target_dir.relative_to(script_dir)}:")
        for item in sorted(tests_target_dir.iterdir()):
            if item.is_dir():
                print(f"  📁 {item.name}/")
            else:
                print(f"  📄 {item.name}")

        # Replace imports in the repository
        replace_dsi_imports(script_dir)

    except Exception as e:
        print(f"ERROR: Failed to copy files: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
