import re
from typing import Optional

UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", re.I)

def mint_uuid(seed: Optional[str]) -> str:
    """
    Deterministic UUID mint (pure): FNV-1a hash of a seed, formatted 8-4-4-4-12
    with version 4 / variant bits. Exactly matches mintUuid from K2 JS.
    """
    h = 2166136261
    s = str(seed or "")
    for ch in s:
        h ^= ord(ch)
        h = (h * 16777619) & 0xFFFFFFFF

    def hex_part(n: int) -> str:
        nonlocal h
        res = []
        for _ in range(n):
            h = ((h ^ (h >> 13)) * 0x5BD1E995) & 0xFFFFFFFF
            res.append(format(h & 15, "x"))
        return "".join(res)

    p1 = hex_part(8)
    p2 = hex_part(4)
    p3 = "4" + hex_part(3)
    variant = (8 + (h % 4)) & 0xF
    p4 = format(variant, "x") + hex_part(3)
    p5 = hex_part(12)
    return f"{p1}-{p2}-{p3}-{p4}-{p5}"
