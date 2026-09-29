#!/usr/bin/env python3
"""Pack PNG payloads into favicon/favicon.ico (stdlib only).

Reads the dark-tile app-icon PNGs at 16, 32 and 48 px and embeds them
unmodified — modern ICO containers carry PNG image data directly.

Usage: python3 build_ico.py [out.ico] [png ...]
Defaults: assets/brand/favicon/favicon.ico from
assets/brand/app-icon/png/cc-lb-app-icon-{16,32,48}.png
"""

import os
import struct
import sys

TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))
BRAND_DIR = os.path.dirname(TOOLS_DIR)

DEFAULT_PNGS = [
    os.path.join(BRAND_DIR, "app-icon", "png", f"cc-lb-app-icon-{n}.png")
    for n in (16, 32, 48)
]
DEFAULT_OUT = os.path.join(BRAND_DIR, "favicon", "favicon.ico")


def png_size(data: bytes) -> tuple[int, int]:
    if data[:8] != b"\x89PNG\r\n\x1a\n":
        raise ValueError("not a PNG")
    return struct.unpack(">II", data[16:24])


def main() -> None:
    argv = sys.argv[1:]
    out = argv[0] if argv else DEFAULT_OUT
    pngs = argv[1:] if len(argv) > 1 else DEFAULT_PNGS

    entries = []
    for path in pngs:
        with open(path, "rb") as fh:
            data = fh.read()
        w, h = png_size(data)
        # ICO stores 256 as 0; our sources never exceed 255 but be explicit.
        entries.append(
            (
                0 if w >= 256 else w,  # width byte
                0 if h >= 256 else h,  # height byte
                data,
            )
        )

    # ICONDIR: reserved, type=1 (icon), count
    header = struct.pack("<HHH", 0, 1, len(entries))
    dir_size = 6 + 16 * len(entries)
    offset = dir_size
    directory = b""
    payload = b""
    for w_b, h_b, data in entries:
        directory += struct.pack(
            "<BBBBHHII",
            w_b,        # bWidth
            h_b,        # bHeight
            0,          # bColorCount (0 = >=8bpp)
            0,          # bReserved
            1,          # wPlanes
            32,         # wBitCount
            len(data),  # dwBytesInRes
            offset,     # dwImageOffset
        )
        payload += data
        offset += len(data)

    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "wb") as fh:
        fh.write(header + directory + payload)
    sizes = ", ".join(f"{w}x{h}" for w, h, _ in entries)
    print(f"{out}  ({sizes})")


if __name__ == "__main__":
    main()
