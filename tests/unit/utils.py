import numpy as np


def make_dat_array_info(entries: dict[str, str]) -> np.ndarray:
    """Helper: create a ZIMPOL-style info array from a dict."""
    rows = []
    for k, v in entries.items():
        rows.append([k.encode("UTF-8"), v.encode("UTF-8")])
    return np.array(rows, dtype=object)
