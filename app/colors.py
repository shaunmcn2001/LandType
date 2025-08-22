import hashlib
from typing import Tuple

def color_from_code(code: str) -> Tuple[int, int, int]:
    """Deterministic pastel-like color from a land type code."""
    h = hashlib.md5(code.encode("utf-8")).hexdigest()
    r = int(h[0:2], 16)
    g = int(h[2:4], 16)
    b = int(h[4:6], 16)
    # Lift toward pastels
    r = int(128 + (r / 255) * 127)
    g = int(128 + (g / 255) * 127)
    b = int(128 + (b / 255) * 127)
    return r, g, b
