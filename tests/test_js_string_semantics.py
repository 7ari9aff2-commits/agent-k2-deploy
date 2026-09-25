"""_js_string semantics — cross-module divergence is deliberate; pin it (2026-09-23).

Two coexisting variants, each correct for its module:
  • JS-faithful (gates, llm_safety, stages_pre, stages_post): None -> 'null',
    _UNDEFINED -> 'undefined' — exact String() parity for state-machine conditions.
  • Empty (orchestrator, agent_output, response_policy): None -> '' — the decision
    table and reply paths need missing values to be FALSY.

The danger is a future edit copying a pattern across the boundary:
  • bare truthiness `if _js_string(x):` in a null-variant module treats a MISSING
    value as present ('null' is truthy);
  • `== ''` comparisons on a null-variant _js_string can never match None.
This test pins both semantics AND greps the null-variant modules for the bare
truthiness pattern, so the bomb cannot be re-armed silently.
"""
from __future__ import annotations

import re
from pathlib import Path

from app.core import gates, llm_safety, orchestrator
from app.core.agent_output import _js_string as js_string_empty
from app.core.response_policy import _js_string as js_string_policy

NULL_VARIANT_MODULES = [
    "app/core/gates.py",
    "app/core/llm_safety.py",
    "app/pipeline/stages_pre.py",
    "app/pipeline/stages_post.py",
]


def test_null_variant_semantics():
    assert gates._js_string(None) == "null"
    assert llm_safety._js_string(None) == "null"
    assert gates._js_string(gates._UNDEFINED) == "undefined"


def test_empty_variant_semantics():
    assert orchestrator._js_string(None) == ""
    assert js_string_empty(None) == ""
    assert js_string_policy(None) == ""


def test_empty_variant_falsy_vs_null_variant_truthy():
    # the asymmetry itself — pinned so any unification is a conscious decision
    assert not orchestrator._js_string(None)
    assert gates._js_string(None)


def test_no_bare_truthiness_in_null_variant_modules():
    """Flag lines that USE _js_string(...) as a bare boolean inside a null-variant
    module (None becomes 'null', which is truthy = a missing value treated as
    present). Lines with an explicit comparison/coercion or a membership check are
    safe — 'null' never matches an explicit tuple, and == '' / != '' are the
    empty-variant idiom."""
    root = Path(__file__).resolve().parents[1]
    truthiness_context = re.compile(r"\bif\s+_js_string\(|\b(?:and|or)\s+_js_string\(")
    safe_markers = ("==", "!=", ".lower()", ".strip()", ".upper()",
                    " is not None", " is None", " in (")
    offenders = []
    for rel in NULL_VARIANT_MODULES:
        lines = (root / rel).read_text(encoding="utf-8").splitlines()
        for line_no, line in enumerate(lines, 1):
            if "_js_string(" not in line or not truthiness_context.search(line):
                continue
            # a call may span a few lines — include up to 3 continuation lines and
            # close any unbalanced parens so trailing markers are still visible
            after = line[line.index("_js_string("):]
            joined = after
            for cont in lines[line_no:line_no + 3]:   # up to 3 lines below (0-based)
                if joined.count("(") > joined.count(")"):
                    joined += " " + cont
                else:
                    break
            if any(marker in joined for marker in safe_markers):
                continue
            offenders.append(f"{rel}:{line_no}: {line.strip()[:120]}")
    assert not offenders, "dangerous _js_string usage:\n" + "\n".join(offenders)
