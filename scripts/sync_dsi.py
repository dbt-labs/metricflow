#!/usr/bin/env python3
"""Script to sync dbt-semantic-interfaces files to metricflow-semantic-interfaces folder.

This script assumes that dbt-semantic-interfaces is cloned in the same parent directory
as this metricflow repository.

The script will:
1. Pull the latest main branch from dbt-semantic-interfaces repository
2. Create a new metricflow-semantic-interfaces folder with the following structure:
   - metricflow-semantic-interfaces/
     - metricflow_semantic_interfaces/ (copied from dbt-semantic-interfaces/dbt_semantic_interfaces/)
     - tests/ (copied from dbt-semantic-interfaces/tests/)
     - msi_pydantic_shim.py (copied from dbt-semantic-interfaces/dsi_pydantic_shim.py)
3. Update import paths within the metricflow-semantic-interfaces folder to use the new structure
4. Update generic noqa comments (like # noqa: D) to be more specific (like # noqa: D103)
5. Run the linter using 'make lint' to check for any issues
6. Check and sync dependencies from dbt-semantic-interfaces to metricflow
"""
from __future__ import annotations

import re
import shutil
import subprocess
import sys
from pathlib import Path


def run_git_command(command: list[str], cwd: Path) -> str:
    """Run a git command in the specified directory."""
    try:
        result = subprocess.run(command, cwd=cwd, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Git command failed: {' '.join(command)}")
        print(f"Error output: {e.stderr}")
        sys.exit(1)


def update_dsi_repository(dsi_repo_path: Path) -> None:
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


def update_noqa_comments_in_directory(directory: Path) -> int:
    """Update generic noqa comments to be more specific for Ruff.

    Args:
        directory: Directory to search for Python files

    Returns:
        Number of files that were modified
    """
    modified_count = 0

    def get_context_specific_d_code(lines: list[str], line_index: int) -> str:
        """Determine the appropriate D code based on the context of the noqa comment.

        Args:
            lines: All lines in the file
            line_index: Index of the line with the noqa comment

        Returns:
            Appropriate D code (D101, D102, D103, etc.)
        """
        # Look backwards from the current line to find the definition
        for i in range(line_index, max(-1, line_index - 10), -1):
            line = lines[i].strip()

            # Skip empty lines and comments
            if not line or line.startswith("#"):
                continue

            # Check for class definition
            if re.match(r"^\s*class\s+\w+", line):  # Match class even without colon (multiline class def)
                # Check if this is a nested class by looking for a parent class
                class_indent = len(lines[i]) - len(lines[i].lstrip())
                for k in range(i - 1, max(-1, i - 500), -1):
                    parent_line = lines[k].strip()
                    if not parent_line or parent_line.startswith("#"):
                        continue
                    if re.match(
                        r"^\s*class\s+\w+", parent_line
                    ):  # Match class even without colon (multiline class def)
                        parent_indent = len(lines[k]) - len(lines[k].lstrip())
                        if class_indent > parent_indent:
                            return "D106"  # Missing docstring in public nested class
                        break
                return "D101"  # Missing docstring in public class

            # Check for function definition
            if re.match(r"^\s*def\s+\w+.*:", line):
                # Extract the function name from this line
                func_match = re.search(r"def\s+(\w+)", line)
                func_name = func_match.group(1) if func_match else ""

                # Determine if it's a method (inside a class) or standalone function
                # Look further back to see if we're inside a class
                current_indent = len(lines[i]) - len(lines[i].lstrip())
                for j in range(i - 1, max(-1, i - 500), -1):
                    class_line = lines[j].strip()
                    # Skip empty lines and comments
                    if not class_line or class_line.startswith("#"):
                        continue
                    if re.match(r"^\s*class\s+\w+", class_line):  # Match class even without colon (multiline class def)
                        # Check if we're actually indented inside this class
                        class_indent = len(lines[j]) - len(lines[j].lstrip())
                        if current_indent > class_indent:
                            # We're inside a class, so this is a method
                            if func_name == "__init__":
                                return "D107"  # Missing docstring in __init__
                            elif func_name.startswith("__") and func_name.endswith("__"):
                                return "D105"  # Missing docstring in magic method
                            else:
                                return "D102"  # Missing docstring in public method
                        # If not indented more than class, it's not inside the class
                    # Don't break on other function definitions - keep looking for class

                # Not inside a class, so it's a standalone function
                return "D103"  # Missing docstring in public function

        # Default fallback
        return "D103"  # Most common case

    # Context-aware noqa patterns for D codes
    # We now analyze context to determine the correct D code automatically:
    # - D101: Missing docstring in public class
    # - D102: Missing docstring in public method
    # - D103: Missing docstring in public function
    # - D105: Missing docstring in magic method
    # - D106: Missing docstring in public nested class
    # - D107: Missing docstring in __init__

    generic_d_patterns = [
        # Handle improper spacing first (no space after #)
        r"#noqa:\s*D(?!\d)",
        # Handle proper spacing but generic codes
        r"# noqa:\s*D(?!\d)",
        # Also handle existing specific D codes that may be incorrect
        r"# noqa:\s*D\d+",
        # Handle empty noqa comments (just # noqa: with nothing after)
        r"# noqa:\s*$",
    ]

    other_noqa_patterns = [
        # Generic F (pyflakes) -> F401 (unused import) - most common case
        (r"#noqa:\s*F(?!\d)", "# noqa: F401"),
        (r"# noqa:\s*F(?!\d)", "# noqa: F401"),
        # Generic E (pycodestyle) -> E501 (line too long) - most common case
        (r"#noqa:\s*E(?!\d)", "# noqa: E501"),
        (r"# noqa:\s*E(?!\d)", "# noqa: E501"),
    ]

    for py_file in directory.rglob("*.py"):
        try:
            with open(py_file, "r", encoding="utf-8") as f:
                content = f.read()

            original_content = content
            lines = content.split("\n")

            # First, handle context-aware D code replacements
            for i, line in enumerate(lines):
                for pattern in generic_d_patterns:
                    if re.search(pattern, line):
                        correct_d_code = get_context_specific_d_code(lines, i)
                        # Replace the generic D with the specific code
                        new_line = re.sub(pattern, f"# noqa: {correct_d_code}", line)
                        lines[i] = new_line

            # Reconstruct content from modified lines
            content = "\n".join(lines)

            # Then handle other generic noqa patterns (F, E, etc.)
            for pattern, replacement in other_noqa_patterns:
                content = re.sub(pattern, replacement, content)

            if content != original_content:
                with open(py_file, "w", encoding="utf-8") as f:
                    f.write(content)
                modified_count += 1
                print(f"  ✏️  Updated noqa comments in {py_file.relative_to(directory.parent)}")

        except Exception as e:
            print(f"  ⚠️  Warning: Failed to process {py_file}: {e}")

    return modified_count


def update_mypy_issues_in_directory(directory: Path) -> int:
    """Fix mypy issues by adding appropriate noqa comments and fixing type annotations.

    Args:
        directory: Directory to search for Python files

    Returns:
        Number of files that were modified
    """
    modified_count = 0

    for py_file in directory.rglob("*.py"):
        try:
            with open(py_file, "r", encoding="utf-8") as f:
                content = f.read()

            lines = content.split("\n")

            # Track if we made any changes
            file_modified = False

            # 1. Add noqa comments for explicit Any usage
            for i, line in enumerate(lines):
                # Skip lines that already have noqa comments
                if "# noqa:" in line or "# type: ignore" in line:
                    continue

                # Check for explicit Any usage (not in import lines)
                if re.search(r"\bAny\b", line) and "import" not in line:
                    # Add type ignore comment for mypy misc error
                    if line.rstrip().endswith(":"):
                        # For function signatures ending with colon
                        lines[i] = line.rstrip() + "  # type: ignore[misc]"
                    else:
                        # For other lines
                        lines[i] = line.rstrip() + "  # type: ignore[misc]"
                    file_modified = True

            # 2. Fix functions missing return type annotations
            for i, line in enumerate(lines):
                # Skip lines that already have noqa comments or type ignore
                if "# noqa:" in line or "# type: ignore" in line:
                    continue

                # Look for function definitions without return type annotations
                # Pattern: def func_name(...): (no -> return_type)
                if re.match(r"^\s*def\s+\w+\([^)]*\)\s*:", line) and "->" not in line:
                    # This function doesn't have a return type annotation
                    if line.rstrip().endswith(":"):
                        # Check if this looks like it should return None (common patterns)
                        func_body_start = i + 1
                        likely_returns_none = True

                        # Look at a few lines after the function to see if it has return statements
                        for j in range(func_body_start, min(len(lines), func_body_start + 10)):
                            body_line = lines[j].strip()
                            if not body_line or body_line.startswith("#"):
                                continue
                            # If we see a return with a value, it probably doesn't return None
                            if re.match(r"^\s*return\s+\w+", body_line):
                                likely_returns_none = False
                                break
                            # If we see just 'return' or 'pass', it likely returns None
                            if body_line in ["return", "pass"] or body_line.startswith("pass"):
                                likely_returns_none = True
                                break
                            # If we see another function definition, stop looking
                            if re.match(r"^\s*def\s+\w+", body_line):
                                break

                        if likely_returns_none:
                            # Add -> None
                            lines[i] = line.rstrip()[:-1] + " -> None:"
                        else:
                            # Add -> Any with type ignore
                            lines[i] = line.rstrip()[:-1] + " -> Any:  # type: ignore[misc]"
                        file_modified = True

            # 3. Fix functions with untyped parameters
            for i, line in enumerate(lines):
                # Skip lines that already have noqa comments
                if "# noqa:" in line or "# type: ignore" in line:
                    continue

                # Look for function definitions that might have untyped parameters
                if re.match(r"^\s*def\s+\w+\(.*\)\s*(->\s*\w+\s*)?:", line):
                    # Check if it has parameters that might be untyped
                    if "(" in line and ")" in line:
                        # Extract parameter part
                        start_paren = line.find("(")
                        end_paren = line.rfind(")")
                        param_part = line[start_paren + 1 : end_paren]

                        if param_part.strip():
                            # Parse parameters and add Any type annotations where missing
                            params = [p.strip() for p in param_part.split(",") if p.strip()]
                            updated_params = []
                            needs_any_import = False

                            for param in params:
                                param = param.strip()
                                if not param:
                                    continue

                                # Skip self, cls, and already typed parameters
                                if param in ["self", "cls"] or ":" in param:
                                    updated_params.append(param)
                                # Skip *args and **kwargs patterns that might be complex
                                elif param.startswith("*"):
                                    updated_params.append(param)
                                else:
                                    # Add Any type annotation for simple parameters only
                                    # Check if it's a simple parameter name (no complex syntax)
                                    if re.match(r"^\w+$", param):
                                        updated_params.append(f"{param}: Any")
                                        needs_any_import = True
                                    else:
                                        # Keep complex parameters as-is
                                        updated_params.append(param)

                            if needs_any_import and updated_params != params:
                                # Reconstruct the function signature
                                new_param_part = ", ".join(updated_params)
                                new_line = line[: start_paren + 1] + new_param_part + line[end_paren:]

                                # Add type ignore comment if we added Any
                                if "# type: ignore" not in new_line:
                                    new_line = new_line.rstrip()
                                    if new_line.endswith(":"):
                                        new_line = new_line[:-1] + ":  # type: ignore[misc]"
                                    else:
                                        new_line = new_line + "  # type: ignore[misc]"

                                lines[i] = new_line
                                file_modified = True

            if file_modified:
                # Check if we need to add Any import
                content_str = "\n".join(lines)
                if ": Any" in content_str or "-> Any" in content_str:
                    # Check if Any is already imported
                    has_any_import = False
                    for line in lines:
                        if re.search(r"\bfrom typing import.*\bAny\b", line) or re.search(r"\bimport.*\bAny\b", line):
                            has_any_import = True
                            break

                    if not has_any_import:
                        # Find existing typing import to add Any to it
                        typing_import_line = -1
                        for j, line in enumerate(lines):
                            if re.match(r"^\s*from typing import", line):
                                typing_import_line = j
                                break

                        if typing_import_line >= 0:
                            # Check if this is a multi-line import
                            import_line = lines[typing_import_line]
                            if "Any" not in import_line:
                                # Check if it's a multi-line import by looking for opening paren without closing
                                if "(" in import_line and ")" not in import_line:
                                    # Multi-line import - find the closing parenthesis
                                    close_paren_line = -1
                                    for k in range(typing_import_line + 1, len(lines)):
                                        if ")" in lines[k]:
                                            close_paren_line = k
                                            break

                                    if close_paren_line > 0:
                                        # Check if Any is already in the multi-line import
                                        full_import = "\n".join(lines[typing_import_line : close_paren_line + 1])
                                        if "Any" not in full_import:
                                            # Insert Any before the closing parenthesis
                                            close_line = lines[close_paren_line]
                                            indent = len(close_line) - len(close_line.lstrip())
                                            lines.insert(close_paren_line, " " * indent + "Any,")
                                elif import_line.rstrip().endswith(")"):
                                    # Single-line with parentheses
                                    lines[typing_import_line] = import_line.rstrip()[:-1] + ", Any)"
                                else:
                                    # Simple single-line import
                                    lines[typing_import_line] = import_line.rstrip() + ", Any"
                        else:
                            # Add new typing import after __future__ imports or at the top
                            insert_line = 0
                            for j, line in enumerate(lines):
                                if line.strip().startswith("from __future__"):
                                    insert_line = j + 1
                                elif line.strip() and not line.strip().startswith("#"):
                                    break
                            lines.insert(insert_line, "from typing import Any")

                # Reconstruct content from modified lines
                content = "\n".join(lines)

                with open(py_file, "w", encoding="utf-8") as f:
                    f.write(content)
                modified_count += 1
                print(f"  ✏️  Fixed mypy issues in {py_file.relative_to(directory.parent)}")

        except Exception as e:
            print(f"  ⚠️  Warning: Failed to process {py_file}: {e}")

    return modified_count


def check_and_sync_dependencies(repo_root: Path, dsi_repo_path: Path) -> None:
    """Check and sync dependencies from dbt-semantic-interfaces to metricflow.

    Args:
        repo_root: Path to metricflow repository root
        dsi_repo_path: Path to dbt-semantic-interfaces repository
    """
    print("\n🔍 Checking dependencies...")

    # Read dbt-semantic-interfaces pyproject.toml
    dsi_pyproject = dsi_repo_path / "pyproject.toml"
    if not dsi_pyproject.exists():
        print("⚠️  dbt-semantic-interfaces pyproject.toml not found")
        return

    # Read metricflow pyproject.toml
    mf_pyproject = repo_root / "pyproject.toml"
    if not mf_pyproject.exists():
        print("⚠️  metricflow pyproject.toml not found")
        return

    try:
        import tomli as tomllib
    except ImportError:
        print("⚠️  toml library not available, skipping dependency check")
        return

    # Parse both pyproject.toml files
    with open(dsi_pyproject, "rb") as f:
        dsi_config = tomllib.load(f)

    with open(mf_pyproject, "rb") as f:
        mf_config = tomllib.load(f)

    # Get dev-env dependencies from dbt-semantic-interfaces
    dsi_dev_deps = {}
    if "tool" in dsi_config and "hatch" in dsi_config["tool"] and "envs" in dsi_config["tool"]["hatch"]:
        if "dev-env" in dsi_config["tool"]["hatch"]["envs"]:
            dev_env = dsi_config["tool"]["hatch"]["envs"]["dev-env"]
            if "dependencies" in dev_env:
                for dep in dev_env["dependencies"]:
                    # Parse dependency (e.g., "pytest>=7.0" -> "pytest")
                    dep_name = re.split(r"[>=<!]", dep)[0].strip()
                    dsi_dev_deps[dep_name] = dep

    # Get dev-env dependencies from metricflow
    mf_dev_deps = {}
    if "tool" in mf_config and "hatch" in mf_config["tool"] and "envs" in mf_config["tool"]["hatch"]:
        if "dev-env" in mf_config["tool"]["hatch"]["envs"]:
            dev_env = mf_config["tool"]["hatch"]["envs"]["dev-env"]
            if "dependencies" in dev_env:
                for dep in dev_env["dependencies"]:
                    dep_name = re.split(r"[>=<!]", dep)[0].strip()
                    mf_dev_deps[dep_name] = dep
            elif "features" in dev_env and "dev-env-requirements" in dev_env["features"]:
                # MetricFlow uses requirements files - check the requirements files
                if "tool" in mf_config and "hatch" in mf_config["tool"] and "metadata" in mf_config["tool"]["hatch"]:
                    metadata = mf_config["tool"]["hatch"]["metadata"]
                    if "hooks" in metadata and "requirements_txt" in metadata["hooks"]:
                        req_hooks = metadata["hooks"]["requirements_txt"]
                        if (
                            "optional-dependencies" in req_hooks
                            and "dev-env-requirements" in req_hooks["optional-dependencies"]
                        ):
                            req_files = req_hooks["optional-dependencies"]["dev-env-requirements"]
                            # Read the requirements files
                            for req_file in req_files:
                                req_path = repo_root / req_file
                                if req_path.exists():
                                    with open(req_path, "r") as f:
                                        for line in f:
                                            line = line.strip()
                                            if line and not line.startswith("#"):
                                                dep_name = re.split(r"[>=<!]", line)[0].strip()
                                                mf_dev_deps[dep_name] = line

    # Find missing dependencies
    missing_deps = []
    for dep_name, dep_spec in dsi_dev_deps.items():
        if dep_name not in mf_dev_deps:
            missing_deps.append(dep_spec)

    if missing_deps:
        print(f"📦 Found {len(missing_deps)} missing dependencies in metricflow dev-env:")
        for dep in missing_deps:
            print(f"  - {dep}")

        # Add missing dependencies to metricflow requirements files
        # Check if metricflow uses requirements files
        if "tool" in mf_config and "hatch" in mf_config["tool"] and "metadata" in mf_config["tool"]["hatch"]:
            metadata = mf_config["tool"]["hatch"]["metadata"]
            if "hooks" in metadata and "requirements_txt" in metadata["hooks"]:
                req_hooks = metadata["hooks"]["requirements_txt"]
                if (
                    "optional-dependencies" in req_hooks
                    and "dev-env-requirements" in req_hooks["optional-dependencies"]
                ):
                    req_files = req_hooks["optional-dependencies"]["dev-env-requirements"]
                    # Add to the first requirements file
                    if req_files:
                        main_req_file = repo_root / req_files[0]  # Use first requirements file
                        if main_req_file.exists():
                            with open(main_req_file, "r") as f:
                                content = f.read()

                            # Add missing dependencies
                            new_content = content.rstrip() + "\n"
                            for dep in missing_deps:
                                new_content += f"{dep}\n"

                            with open(main_req_file, "w") as f:
                                f.write(new_content)

                            print(
                                f"✅ Added {len(missing_deps)} missing dependencies to {main_req_file.relative_to(repo_root)}"
                            )
                        else:
                            print(f"⚠️  Requirements file {main_req_file} not found")
                    else:
                        print("⚠️  No requirements files found in metricflow config")
                else:
                    print("⚠️  dev-env-requirements not found in metricflow config")
            else:
                print("⚠️  requirements_txt hooks not found in metricflow config")
        else:
            # Fallback to adding to pyproject.toml (original logic)
            with open(mf_pyproject, "r") as f:
                content = f.read()

            # Find the dev-env dependencies section and add missing deps
            # This is a simple approach - we'll add them at the end of the dependencies list
            dev_env_pattern = r"(\[tool\.hatch\.envs\.dev-env\]\s*dependencies\s*=\s*\[)(.*?)(\])"
            match = re.search(dev_env_pattern, content, re.DOTALL)

            if match:
                prefix = match.group(1)
                existing_deps = match.group(2)
                suffix = match.group(3)

                # Add missing dependencies
                new_deps = existing_deps.rstrip().rstrip(",")
                for dep in missing_deps:
                    new_deps += f',\n  "{dep}"'

                new_content = content[: match.start()] + prefix + new_deps + "\n" + suffix + content[match.end() :]

                with open(mf_pyproject, "w") as f:
                    f.write(new_content)

                print(f"✅ Added {len(missing_deps)} missing dependencies to metricflow pyproject.toml")
            else:
                print("⚠️  Could not find dev-env dependencies section in metricflow pyproject.toml")
    else:
        print("✅ All dbt-semantic-interfaces dev-env dependencies are already in metricflow")


def run_linter(repo_root: Path) -> None:
    """Run the linter using make lint command.

    Args:
        repo_root: Root directory of the repository
    """
    print("\n🔍 Running linter with 'make lint'...")
    try:
        result = subprocess.run(
            ["make", "lint"], cwd=repo_root, capture_output=True, text=True, timeout=300  # 5 minute timeout
        )

        if result.returncode == 0:
            print("✅ Linter passed successfully!")
            if result.stdout.strip():
                print("Linter output:")
                print(result.stdout)
        else:
            print(f"⚠️  Linter found issues (exit code: {result.returncode})")
            if result.stdout.strip():
                print("Linter stdout:")
                print(result.stdout)
            if result.stderr.strip():
                print("Linter stderr:")
                print(result.stderr)

    except subprocess.TimeoutExpired:
        print("⚠️  Linter timed out after 5 minutes")
    except Exception as e:
        print(f"⚠️  Failed to run linter: {e}")


def update_imports_in_directory(directory: Path, old_import: str, new_import: str) -> int:
    """Update import statements in all Python files within a directory.

    Args:
        directory: Directory to search for Python files
        old_import: Old import pattern to replace (e.g., 'dbt_semantic_interfaces')
        new_import: New import pattern (e.g., 'metricflow_semantic_interfaces')

    Returns:
        Number of files that were modified
    """
    modified_count = 0

    for py_file in directory.rglob("*.py"):
        try:
            with open(py_file, "r", encoding="utf-8") as f:
                content = f.read()

            original_content = content

            # Replace import patterns (only at word boundaries to avoid partial matches):
            # 1. from dbt_semantic_interfaces.xxx import yyy
            # 2. import dbt_semantic_interfaces.xxx
            # 3. from dbt_semantic_interfaces import xxx

            patterns = [
                (rf"\bfrom {re.escape(old_import)}(\.[\w.]+)? import", rf"from {new_import}\1 import"),
                (rf"\bimport {re.escape(old_import)}(\.[\w.]+)?", rf"import {new_import}\1"),
                (rf"\bfrom {re.escape(old_import)} import", rf"from {new_import} import"),
            ]

            for pattern, replacement in patterns:
                content = re.sub(pattern, replacement, content)

            if content != original_content:
                with open(py_file, "w", encoding="utf-8") as f:
                    f.write(content)
                modified_count += 1
                print(f"  ✏️  Updated imports in {py_file.relative_to(directory.parent)}")

        except Exception as e:
            print(f"  ⚠️  Warning: Failed to process {py_file}: {e}")

    return modified_count


def main() -> None:
    """Main function to copy dbt-semantic-interfaces files."""
    # Get the metricflow repo root directory (parent of scripts directory)
    script_dir = Path(__file__).parent.absolute()
    repo_root = script_dir.parent

    # Assume dbt-semantic-interfaces is in the same parent directory as metricflow
    parent_dir = repo_root.parent
    dsi_repo_path = parent_dir / "dbt-semantic-interfaces"

    # Target directories in this repo
    msi_root_dir = repo_root / "metricflow-semantic-interfaces"
    target_code_dir = msi_root_dir / "metricflow_semantic_interfaces"
    target_tests_dir = msi_root_dir / "tests"

    print(f"Script directory: {script_dir}")
    print(f"Repo root directory: {repo_root}")
    print(f"Looking for dbt-semantic-interfaces at: {dsi_repo_path}")
    print(f"Target root directory: {msi_root_dir}")
    print(f"Target directory for code: {target_code_dir}")
    print(f"Target directory for tests: {target_tests_dir}")

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

    # Create target directories if they don't exist, or clean them if they do
    if msi_root_dir.exists():
        print(f"Target directory {msi_root_dir} already exists. Contents will be replaced.")
        shutil.rmtree(msi_root_dir)

    msi_root_dir.mkdir(parents=True, exist_ok=True)
    target_code_dir.mkdir(parents=True, exist_ok=True)
    target_tests_dir.mkdir(parents=True, exist_ok=True)
    print(f"Created target directories: {msi_root_dir}")

    # Copy the folders and file
    try:
        print("Copying dbt_semantic_interfaces contents to metricflow_semantic_interfaces...")
        # Copy contents of dbt_semantic_interfaces directly to target_code_dir
        for item in source_dbt_semantic_interfaces.iterdir():
            if item.is_dir():
                shutil.copytree(item, target_code_dir / item.name)
            else:
                shutil.copy2(item, target_code_dir / item.name)

        print("Copying tests contents to metricflow-semantic-interfaces/tests/...")
        # Copy contents of tests to target_tests_dir
        for item in source_tests.iterdir():
            if item.is_dir():
                shutil.copytree(item, target_tests_dir / item.name)
            else:
                shutil.copy2(item, target_tests_dir / item.name)

        print("Copying and renaming dsi_pydantic_shim.py -> msi_pydantic_shim.py...")
        shutil.copy2(source_pydantic_shim, msi_root_dir / "msi_pydantic_shim.py")

        # Create __init__.py files to make it a proper Python package
        print("Creating __init__.py files...")
        (target_code_dir / "__init__.py").touch()
        (target_tests_dir / "__init__.py").touch()

        print("✅ Successfully copied all files!")
        print(f"Root directory: {msi_root_dir}")
        print(f"Code files copied to: {target_code_dir}")
        print(f"Test files copied to: {target_tests_dir}")

        # Update imports within the metricflow-semantic-interfaces folder
        print("\n🔄 Updating import paths within metricflow-semantic-interfaces folder...")
        modified_files = update_imports_in_directory(
            msi_root_dir, "dbt_semantic_interfaces", "metricflow_semantic_interfaces"
        )

        # Update dsi_pydantic_shim imports to msi_pydantic_shim
        modified_shim_files = update_imports_in_directory(msi_root_dir, "dsi_pydantic_shim", "msi_pydantic_shim")

        print(f"✅ Updated imports in {modified_files} files")
        print(f"✅ Updated pydantic shim imports in {modified_shim_files} files")

        # Update noqa comments to be more specific
        print("\n🔄 Updating noqa comments to be more specific...")
        modified_noqa_files = update_noqa_comments_in_directory(msi_root_dir)
        print(f"✅ Updated noqa comments in {modified_noqa_files} files")

        # Fix mypy issues
        print("\n🔧 Fixing mypy issues...")
        modified_mypy_files = update_mypy_issues_in_directory(msi_root_dir)
        print(f"✅ Fixed mypy issues in {modified_mypy_files} files")

        # Check and sync dependencies
        check_and_sync_dependencies(repo_root, dsi_repo_path)

        # Run linter
        run_linter(repo_root)

        # List what was copied
        print(f"\n📁 Contents of {msi_root_dir.name}:")
        for item in sorted(msi_root_dir.iterdir()):
            if item.is_dir():
                print(f"  📁 {item.name}/")
                # Show a few items from subdirectories
                sub_items = list(item.iterdir())[:5]
                for sub_item in sub_items:
                    prefix = "📁" if sub_item.is_dir() else "📄"
                    print(f"    {prefix} {sub_item.name}")
                if len(list(item.iterdir())) > 5:
                    print(f"    ... and {len(list(item.iterdir())) - 5} more items")
            else:
                print(f"  📄 {item.name}")

    except Exception as e:
        print(f"ERROR: Failed to copy files: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
