"""ZIMPOL3 Binary Data (Z3BD) header reader.

Reads only the metadata header from ``.z3bd`` raw camera files.
Used to extract limbguider coordinates when available.
"""

from __future__ import annotations

import re
from pathlib import Path


def read_z3bd_header(path: Path) -> dict[str, object] | None:
    """Read the metadata header from a Z3BD file.

    Args:
        path: Path to a ``.z3bd`` file.

    Returns:
        Dictionary of header key-value pairs, or ``None`` if the file
        cannot be parsed.
    """
    SOH = b"\x01"
    STX = b"\x02"

    try:
        with Path(path).open("rb") as fid:
            first_byte = fid.read(1)
            if first_byte != SOH:
                return None

            # Read until STX marker
            raw = first_byte
            while True:
                byte = fid.read(1)
                if len(byte) != 1:
                    return None
                raw += byte
                if byte == STX:
                    break

            header_str = raw.decode("iso-8859-1")
    except OSError:
        return None

    m = re.match(
        r"\001(?P<ts>[usf][123468]+)(?P<dl>\[.*?\])(?P<al>[^\002]*)\002",
        header_str,
    )
    if m is None:
        return None

    al = m.group("al")
    header: dict[str, object] = {}
    i, n = 0, len(al)
    while i < n:
        attr_match = re.match(
            r"^ (?P<nam>[a-zA-Z][a-zA-Z0-9_]*)=(?P<val>"
            r"(\{[^{}]*(\{[^{}]*\})*[^{}]*\})"
            r'|("[^"]*")'
            r"|(nan)"
            r"|([-+]?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?))",
            al[i:n],
        )
        if not attr_match:
            break
        name = attr_match.group("nam")
        val_str = attr_match.group("val")
        i += attr_match.span()[1]

        if val_str == "nan":
            value: object = None
        elif val_str.startswith(("{", '"')):
            value = val_str[1:-1]
        else:
            try:
                value = (
                    int(val_str)
                    if "." not in val_str and "e" not in val_str.lower()
                    else float(val_str)
                )
            except ValueError:
                value = val_str

        header[name] = value

    return header
