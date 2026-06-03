import os


_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


def _load_lines(filename: str) -> list[str]:
    path = os.path.join(_DATA_DIR, filename)
    with open(path) as f:
        return [line.strip() for line in f if line.strip()]


_first_names: list[str] | None = None
_last_names: list[str] | None = None


def first_names() -> list[str]:
    global _first_names
    if _first_names is None:
        _first_names = _load_lines("first_names.txt")
    return _first_names


def last_names() -> list[str]:
    global _last_names
    if _last_names is None:
        _last_names = _load_lines("last_names.txt")
    return _last_names