# app/colors.py
import hashlib
from typing import Tuple


# Deterministic color from code string; returns (R,G,B) 0-255
def color_from_code(code: str) -> Tuple[int,int,int]:
    s = (code or "UNK").encode("utf-8")
    h = hashlib.sha1(s).hexdigest()
    r = 60 + (int(h[0:2], 16) % 156)
    g = 60 + (int(h[2:4], 16) % 156)
    b = 60 + (int(h[4:6], 16) % 156)
    return (r, g, b)
