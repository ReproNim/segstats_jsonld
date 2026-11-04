"""Every third-party module the package imports is declared in setup.py, not
only reachable through another dependency (rapidfuzz and requests arrived
only through pynidm)."""
import ast
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
# import name -> distribution name, where they differ
DISTRIBUTION = {"nidm": "pynidm"}


def _install_requires():
    tree = ast.parse((REPO_ROOT / "setup.py").read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.keyword) and node.arg == "install_requires":
            return {ast.literal_eval(e).split("[")[0].split(">")[0].split("<")[0].split("=")[0].strip().lower()
                    for e in node.value.elts}
    raise AssertionError("no install_requires in setup.py")


def _imported_top_level_modules():
    modules = set()
    for path in (REPO_ROOT / "segstats_jsonld").rglob("*.py"):
        if "tests" in path.parts:
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Import):
                modules.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                modules.add(node.module.split(".")[0])
    return modules


def test_every_third_party_import_is_declared():
    third_party = {m for m in _imported_top_level_modules()
                   if m not in sys.stdlib_module_names and m not in ("segstats_jsonld", "__future__")}
    # positive control: the scan sees imports that are declared
    assert {"pandas", "rdflib", "nidm"} <= third_party
    declared = _install_requires()
    missing = {DISTRIBUTION.get(m, m) for m in third_party} - declared
    assert missing == set()
