"""
scripts/validate_dag_registry.py

CI utility: validates that every DAG defined in dags/*.py is documented
in this service's README.md.

Exits with code 0 on success, 1 if any DAG is undocumented.

Usage:
    uv run python scripts/validate_dag_registry.py
"""

import ast
import os
import re
import sys

DAGS_DIR = os.path.join(os.path.dirname(__file__), "..", "dags")
README_PATH = os.path.join(os.path.dirname(__file__), "..", "README.md")


def extract_dag_ids_from_file(path: str) -> list[str]:
    """Return all dag_id string literals assigned in a DAG Python file."""
    dag_ids = []
    try:
        with open(path, encoding="utf-8") as fh:
            source = fh.read()
        tree = ast.parse(source, filename=path)
    except SyntaxError as exc:
        # Let the exception propagate naturally — ast.parse() includes the
        # filename and line number in the SyntaxError message, giving CI a
        # clear and non-duplicated error message.
        raise SyntaxError(f"Failed to parse DAG file {path}: {exc}") from exc

    for node in ast.walk(tree):
        # Match:  dag_id="some_dag"  (keyword argument)
        if isinstance(node, ast.keyword) and node.arg == "dag_id":
            if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                dag_ids.append(node.value.value)
        # Match:  DAG("some_dag", ...)  (positional first argument)
        if isinstance(node, ast.Call):
            func = node.func
            func_name = (
                func.id
                if isinstance(func, ast.Name)
                else (func.attr if isinstance(func, ast.Attribute) else "")
            )
            if func_name == "DAG" and node.args:
                first = node.args[0]
                if isinstance(first, ast.Constant) and isinstance(first.value, str):
                    dag_ids.append(first.value)

    return list(dict.fromkeys(dag_ids))  # deduplicate, preserve order


def get_all_dag_ids() -> dict[str, list[str]]:
    """Map filename → list of dag_ids for every .py file in dags/."""
    result: dict[str, list[str]] = {}
    for fname in sorted(os.listdir(DAGS_DIR)):
        if not fname.endswith(".py") or fname.startswith("_"):
            continue
        full_path = os.path.join(DAGS_DIR, fname)
        ids = extract_dag_ids_from_file(full_path)
        if ids:
            result[fname] = ids
    return result


def get_documented_dag_ids(readme_path: str) -> set[str]:
    """Extract backtick-quoted identifiers that look like dag_ids from the README."""
    with open(readme_path, encoding="utf-8") as fh:
        content = fh.read()
    # Match `dag_id_name` patterns (lower-case words joined by underscores)
    return set(re.findall(r"`([a-z][a-z0-9_]*)`", content))


def main() -> int:
    dag_map = get_all_dag_ids()
    if not dag_map:
        print("ERROR: No DAG files found in", DAGS_DIR)
        return 1

    documented = get_documented_dag_ids(README_PATH)

    all_ok = True
    for fname, ids in dag_map.items():
        for dag_id in ids:
            if dag_id in documented:
                print(f"  ✅  {dag_id}  ({fname})")
            else:
                print(f"  ❌  {dag_id}  ({fname})  — NOT documented in README.md")
                all_ok = False

    if all_ok:
        print("\nAll DAGs are documented in README.md.")
        return 0
    else:
        print(
            "\nSome DAGs are missing from README.md.\n"
            "Add them to the DAG inventory table under '## 🚀 Key Workflows (DAGs)'."
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
