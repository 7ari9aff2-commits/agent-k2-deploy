"""JavaScript semantics used by the n8n port - one shared implementation per helper.

WHY THIS MODULE EXISTS
----------------------
The pipeline is a 1:1 port of an n8n JavaScript graph, so it constantly needs JS
semantics (truthiness, ``||`` chains, ``String()`` coercion, UTF-16 string indices).
Those helpers had been copy-pasted into every module that needed them: 51 definitions
for a handful of one-line primitives.

THE TRAP THIS MODULE IS CAREFUL ABOUT
-------------------------------------
The copies were NOT all the same. Verified by comparing normalised function bodies:

    helper        modules   distinct implementations
    _js_string        10                7
    _js_number         8                5
    _prop              9                4
    _truthy            8                3
    _dig               7                3
    _js_or             9                3
    _js_trim           5                2

``_prop``/``_dig`` return ``None`` in ``agent_output`` but ``_UNDEFINED`` in
``gates``/``llm_safety``; ``_js_string(None)`` is ``""`` in ``agent_output`` but
``"null"`` in ``gates``. Those are real behavioural differences, not formatting.

So this module hosts ONLY the helpers whose bodies are byte-identical across every
module that defines them. The divergent ones (_js_string, _js_number, _prop, _dig,
_js_trim) deliberately stay where they are until each call site is checked - unifying
them blindly would silently change the booking decision core.

Every body below is a VERBATIM copy of the canonical implementation (only the function
name and internal references were rewritten), which is what makes the migration
provable: a local definition was deleted only when it matched byte-for-byte.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

_UNDEFINED = object()

# ---- _truthy -> truthy ----------------------------------------------------
def truthy(value):
    """JS truthiness: {} and [] are truthy; NaN is falsy; 0/''/None/False are falsy."""
    if isinstance(value, float) and value != value:  # NaN
        return False
    if isinstance(value, (dict, list)):
        return True
    return bool(value)


# ---- _js_or -> js_or -----------------------------------------------------
def js_or(*values):
    """JS ``a || b || c`` chain: first JS-truthy value, else the last value (or None)."""
    if not values:
        return None
    for v in values[:-1]:
        if truthy(v):
            return v
    return values[-1]


# ---- _js_and -> js_and ----------------------------------------------------
def js_and(a, b):
    """JS ``a && b``: returns a when a is falsy, else b."""
    return a if not truthy(a) else b


# ---- _dict -> dict_or_empty ---------------------------------------------
def dict_or_empty(value):
    """Property-access coercion: non-object values read as empty objects (JS never throws here)."""
    return value if isinstance(value, dict) else {}


# ---- _obj_or_empty -> obj_or_empty ----------------------------------------------
def obj_or_empty(value: Any) -> Any:
    """JS `x && typeof x === 'object' ? x : {}` (dict/list pass, including [])."""
    if isinstance(value, (dict, list)):
        return value
    return {}


# ---- _first_not_none -> first_not_none --------------------------------------------
def first_not_none(*values):
    """JS ``a ?? b ?? c`` chain: first value that is not null/undefined."""
    for v in values:
        if v is not None:
            return v
    return None


# ---- _is_finite -> is_finite -------------------------------------------------
def is_finite(value):
    """JS Number.isFinite()."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    return not (isinstance(value, float) and (value != value or value in (float('inf'), float('-inf'))))


# ---- _is_finite_num -> is_finite_num ---------------------------------------------
def is_finite_num(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


# ---- _js_is_integer -> js_is_integer ---------------------------------------------
def js_is_integer(value):
    """JS Number.isInteger()."""
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    if isinstance(value, float):
        return value.is_integer()
    return False


# ---- _cp_to_u16 -> cp_to_u16 -------------------------------------------------
def cp_to_u16(s, cp_index):
    """Convert a Python code-point index into the JS UTF-16 code-unit index."""
    return sum(2 if ord(ch) > 0xFFFF else 1 for ch in s[:cp_index])


# ---- _js_len -> js_len ----------------------------------------------------
def js_len(s):
    """JS str .length — UTF-16 code units (astral code points count as 2)."""
    return sum(2 if ord(ch) > 0xFFFF else 1 for ch in s)


# ---- _u16_index_of -> u16_index_of ----------------------------------------------
def u16_index_of(s, sub):
    """JS String.prototype.indexOf — UTF-16 code-unit index, -1 when absent."""
    pos = s.find(sub)
    return -1 if pos < 0 else cp_to_u16(s, pos)


# ---- _string_equals -> string_equals ---------------------------------------------
def string_equals(left: Any, right: str) -> bool:
    """n8n v2 string operator 'equals' (caseSensitive: true): left === right."""
    return isinstance(left, str) and left == right


# ---- _iso_from_ms -> iso_from_ms -----------------------------------------------
def iso_from_ms(ms):
    """JS new Date(ms).toISOString() — always millisecond precision with a 'Z' suffix."""
    try:
        dt = datetime.fromtimestamp(ms // 1000, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None
    return dt.strftime('%Y-%m-%dT%H:%M:%S') + '.%03dZ' % (ms % 1000)


