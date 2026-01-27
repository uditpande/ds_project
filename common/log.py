import os
import sys
import time

NO_COLOR = os.getenv("NO_COLOR") == "1"

COL = {
    "RESET": "\033[0m",
    "RED": "\033[31m",
    "GREEN": "\033[32m",
    "YELLOW": "\033[33m",
    "CYAN": "\033[36m",
}

def _color(s: str, c: str) -> str:
    if NO_COLOR or not sys.stdout.isatty():
        return s
    return f"{COL[c]}{s}{COL['RESET']}"

def log(role: str, node_id: str, event: str, level: str = "INFO", **fields):
    ts = f"{time.time():.3f}"
    base = f"ts={ts} role={role} id={node_id} lvl={level} event={event}"

    if fields:
        parts = []
        for k in sorted(fields.keys()):
            v = fields[k]
            if isinstance(v, tuple):
                v = f"{v[0]}:{v[1]}"
            parts.append(f"{k}={v}")
        base += " " + " ".join(parts)

    if level == "ERROR":
        print(_color(base, "RED"))
    elif level == "WARN":
        print(_color(base, "YELLOW"))
    elif level == "OK":
        print(_color(base, "GREEN"))
    else:
        print(_color(base, "CYAN"))
