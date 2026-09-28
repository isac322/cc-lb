#!/usr/bin/env python3
"""Emit all mark/, app-icon/ and favicon/ SVG assets for the cc-lb brand.

Geometry and palette come from brand.py in this directory — change the mark
there and rebuild via build_all.sh. Every SVG is self-contained: explicit hex
colours, no CSS var(), no live <text>, no external refs.

Usage:
    python3 build_marks.py                     # write the canonical SVG set
    python3 build_marks.py --scratch-tiles D   # also write integer-snapped
                                               # tile-16/32.svg into D (used
                                               # by build_all.sh for 16/32 px)
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import brand  # noqa: E402

BRAND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

COMMENT = (
    "<!-- two squared 'c' brackets form a gate; one lane pierces straight through it -->"
)


def svg_open(width: int, height: int, view: int) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {view} {view}" '
        f'width="{width}" height="{height}">'
    )


def master_parts(ink: str, accent: str) -> str:
    x, y, w, h = brand.MASTER_BAR
    return (
        f'<path fill="{ink}" d="{brand.MASTER_LEFT}"/>'
        f'<path fill="{ink}" d="{brand.MASTER_RIGHT}"/>'
        f'<rect fill="{accent}" x="{x}" y="{y}" width="{w}" height="{h}"/>'
    )


def small_parts(ink: str, accent: str) -> str:
    x, y, w, h = brand.SMALL_BAR
    return (
        f'<path fill="{ink}" d="{brand.SMALL_LEFT}"/>'
        f'<path fill="{ink}" d="{brand.SMALL_RIGHT}"/>'
        f'<rect fill="{accent}" x="{x}" y="{y}" width="{w}" height="{h}"/>'
    )


def mark_svg(palette: dict, small: bool = False, size: int | None = None) -> str:
    """Standalone mark on transparency."""
    view = brand.SMALL_GRID if small else brand.MASTER_GRID
    out_size = size if size is not None else view
    parts = (
        small_parts(palette["ink"], palette["accent"])
        if small
        else master_parts(palette["ink"], palette["accent"])
    )
    return f"{svg_open(out_size, out_size, view)}\n  {COMMENT}\n  {parts}\n</svg>\n"


def mono_svg(colour: str) -> str:
    """Single-colour silhouette (bar merged into the bracket colour)."""
    x, y, w, h = brand.MASTER_BAR
    parts = (
        f'<path fill="{colour}" d="{brand.MASTER_LEFT}"/>'
        f'<path fill="{colour}" d="{brand.MASTER_RIGHT}"/>'
        f'<rect fill="{colour}" x="{x}" y="{y}" width="{w}" height="{h}"/>'
    )
    return (
        f"{svg_open(brand.MASTER_GRID, brand.MASTER_GRID, brand.MASTER_GRID)}\n"
        f"  {parts}\n</svg>\n"
    )


def currentcolor_svg() -> str:
    """currentColor variant for embedding in text/UI contexts."""
    x, y, w, h = brand.MASTER_BAR
    parts = (
        f'<path fill="currentColor" d="{brand.MASTER_LEFT}"/>'
        f'<path fill="currentColor" d="{brand.MASTER_RIGHT}"/>'
        f'<rect fill="currentColor" x="{x}" y="{y}" width="{w}" height="{h}"/>'
    )
    return (
        f"{svg_open(brand.MASTER_GRID, brand.MASTER_GRID, brand.MASTER_GRID)}\n"
        f"  {parts}\n</svg>\n"
    )


def tile_svg(palette: dict, small: bool = False) -> str:
    """Rounded app-icon tile (radius ~22% of the edge, 1-unit border).

    The 64- or 16-grid mark sits centred at 42/64 of the tile, matching the
    R9 tile proportions.
    """
    view = brand.SMALL_GRID if small else brand.MASTER_GRID
    parts = (
        small_parts(palette["ink"], palette["accent"])
        if small
        else master_parts(palette["ink"], palette["accent"])
    )
    tile = brand.MASTER_GRID  # tile canvas stays on the 64 grid
    inner = 42
    off = (tile - inner) // 2
    radius = round(tile * 0.22)
    return (
        f"{svg_open(512, 512, tile)}\n"
        f'  <rect x="0.5" y="0.5" width="{tile - 1}" height="{tile - 1}" '
        f'rx="{radius}" fill="{palette["panel"]}" '
        f'stroke="{palette["border"]}" stroke-width="1"/>\n'
        f'  <svg x="{off}" y="{off}" width="{inner}" height="{inner}" '
        f'viewBox="0 0 {view} {view}">\n'
        f"    {parts}\n"
        f"  </svg>\n</svg>\n"
    )

def tile_svg_integer(palette: dict, size: int) -> str:
    """Pixel-snapped tile for a specific raster size.

    Canvas is the output size itself (viewBox 0 0 size size) and the
    16-grid mark is drawn at integer scale (size/16) anchored at (0, 0),
    so every mark edge lands exactly on a pixel boundary. rx is ~22% of
    the edge (16 -> 3, 32 -> 7).
    """
    radius = max(2, int(size * 0.22))
    parts = small_parts(palette["ink"], palette["accent"])
    return (
        f"{svg_open(size, size, size)}\n"
        f'  <rect x="0.5" y="0.5" width="{size - 1}" height="{size - 1}" '
        f'rx="{radius}" fill="{palette["panel"]}" '
        f'stroke="{palette["border"]}" stroke-width="1"/>\n'
        f'  <svg x="0" y="0" width="{size}" height="{size}" '
        f'viewBox="0 0 {brand.SMALL_GRID} {brand.SMALL_GRID}">\n'
        f"    {parts}\n"
        f"  </svg>\n</svg>\n"
    )


def maskable_svg(palette: dict) -> str:
    """Full-bleed square icon; mark kept inside the maskable safe zone.

    W3C maskable icons may be cropped to any shape inscribed in the central
    circle whose diameter is 80% of the canvas. The mark is scaled to 75% of
    the canvas (48/64), which keeps its bounding box fully inside that circle
    plus margin.
    """
    tile = brand.MASTER_GRID
    inner = 48
    off = (tile - inner) // 2
    parts = master_parts(palette["ink"], palette["accent"])
    return (
        f"{svg_open(512, 512, tile)}\n"
        f'  <rect width="{tile}" height="{tile}" fill="{palette["bg"]}"/>\n'
        f'  <svg x="{off}" y="{off}" width="{inner}" height="{inner}" '
        f'viewBox="0 0 {tile} {tile}">\n'
        f"    {parts}\n"
        f"  </svg>\n</svg>\n"
    )


def favicon_svg() -> str:
    """Adaptive favicon: 16-grid geometry, switches on prefers-color-scheme.

    Default colours are safe on light browser chrome (dark mark); dark
    chrome gets the DARK palette (light mark).
    """
    g = brand.SMALL_GRID
    x, y, w, h = brand.SMALL_BAR
    return (
        f"{svg_open(g, g, g)}\n"
        f"  <style>\n"
        f"    .ink {{ fill: {brand.LIGHT['ink']}; }}\n"
        f"    .accent {{ fill: {brand.LIGHT['accent']}; }}\n"
        f"    @media (prefers-color-scheme: dark) {{\n"
        f"      .ink {{ fill: {brand.DARK['ink']}; }}\n"
        f"      .accent {{ fill: {brand.DARK['accent']}; }}\n"
        f"    }}\n"
        f"  </style>\n"
        f'  <path class="ink" d="{brand.SMALL_LEFT}"/>\n'
        f'  <path class="ink" d="{brand.SMALL_RIGHT}"/>\n'
        f'  <rect class="accent" x="{x}" y="{y}" width="{w}" height="{h}"/>\n'
        f"</svg>\n"
    )


def write(rel: str, content: str) -> str:
    path = os.path.join(BRAND_DIR, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)
    print(path)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scratch-tiles",
        metavar="DIR",
        help="also write integer-snapped dark tile SVGs tile-16.svg and "
        "tile-32.svg into DIR (used by build_all.sh to rasterise crisp "
        "16/32 px PNGs with every mark edge on a pixel boundary)",
    )
    args = parser.parse_args()

    write("mark/cc-lb-mark-on-dark.svg", mark_svg(brand.DARK))
    write("mark/cc-lb-mark-on-light.svg", mark_svg(brand.LIGHT))
    write("mark/cc-lb-mark-mono-black.svg", mono_svg(brand.MONO_BLACK))
    write("mark/cc-lb-mark-mono-white.svg", mono_svg(brand.MONO_WHITE))
    write("mark/cc-lb-mark-currentcolor.svg", currentcolor_svg())
    write("mark/cc-lb-mark-16-on-dark.svg", mark_svg(brand.DARK, small=True))
    write("mark/cc-lb-mark-16-on-light.svg", mark_svg(brand.LIGHT, small=True))
    write("app-icon/cc-lb-app-icon-dark.svg", tile_svg(brand.DARK))
    write("app-icon/cc-lb-app-icon-light.svg", tile_svg(brand.LIGHT))
    write("app-icon/cc-lb-app-icon-maskable.svg", maskable_svg(brand.DARK))
    write("favicon/favicon.svg", favicon_svg())

    if args.scratch_tiles:
        os.makedirs(args.scratch_tiles, exist_ok=True)
        for size in (16, 32):
            path = os.path.join(args.scratch_tiles, f"tile-{size}.svg")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(tile_svg_integer(brand.DARK, size))
            print(path)


if __name__ == "__main__":
    main()
