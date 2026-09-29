#!/usr/bin/env python3
"""Emit the composed brand SVGs: wordmark, lockups, README hero, social preview.

Pure stdlib. Glyph outlines come from wordmark.py (baked from the Hanken
Grotesk variable font); mark geometry and palette from brand.py. Run:

    python3 assets/brand/tools/build_compositions.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import brand  # noqa: E402
import wordmark as wm  # noqa: E402

BRAND = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Wordmark tracking in font units (UPM 1000): -30 ~ -0.9px at 30px, matching
# the original lockup's letter-spacing; the hyphen gets a little air back.
TRACK = -30.0
HYPHEN_PAD = 18.0


def n(v):
    """Compact number for SVG attributes (4 dp: scales like 0.037 stay exact)."""
    s = f"{v:.4f}".rstrip("0").rstrip(".")
    return "0" if s in ("", "-0") else s


def svg_doc(w, h, body):
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {n(w)} {n(h)}" '
        f'width="{n(w)}" height="{n(h)}">{body}</svg>\n'
    )


def write(rel, content):
    path = os.path.join(BRAND, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        fh.write(content)
    print("wrote", os.path.relpath(path))


def mark(x, y, s, pal):
    """The let-gate mark: 64-grid scaled by s; (x, y) is the GRID top-left.

    Returns (svg, ink_box) where ink_box is the drawn-ink rect (x0,y0,x1,y1):
    brackets span grid x 11..53, the bar spans grid y 7..57.
    """
    b = brand.MASTER_BAR
    svg = (
        f'<g transform="translate({n(x)} {n(y)}) scale({n(s)})">'
        f'<path fill="{pal["ink"]}" d="{brand.MASTER_LEFT}"/>'
        f'<path fill="{pal["ink"]}" d="{brand.MASTER_RIGHT}"/>'
        f'<rect fill="{pal["accent"]}" x="{n(b[0])}" y="{n(b[1])}" '
        f'width="{n(b[2])}" height="{n(b[3])}"/></g>'
    )
    return svg, (x + 11 * s, y + 7 * s, x + 53 * s, y + 57 * s)


def text_g(wd, text, size, fill_of, track=0.0, gap_extra=None):
    """Lay out outlined glyphs. Returns (svg, ink_box).

    The group's origin is the text's ink-left on the baseline: svg may be
    embedded via <g transform="translate(tx ty)">. Ink box is relative to
    that origin (top is negative - it sits above the baseline).
    GPOS kerning and tracking are applied between glyphs; gap_extra adds
    space in the gap AFTER glyph index i (keyed by i).
    """
    items = []  # (char, d, pen_x, bounds)
    pen = 0.0
    for i, ch in enumerate(text):
        d, adv, b = wd["glyphs"][ch]
        items.append((ch, d, pen, b))
        if i + 1 < len(text):
            pen += adv + wd["kern"].get((ch, text[i + 1]), 0) + track
            if gap_extra:
                pen += gap_extra.get(i, 0.0)
    x0 = min(x + b[0] for _, _, x, b in items if b)
    y0 = min(b[1] for _, _, _, b in items if b)
    x1 = max(x + b[2] for _, _, x, b in items if b)
    y1 = max(b[3] for _, _, _, b in items if b)
    s = size / wm.UPM
    runs = []  # (fill, [path frags]) merged in order
    for ch, d, x, _b in items:
        if not d:
            continue
        fill = fill_of(ch)
        frag = f'<path transform="translate({n(x - x0)})" d="{d}"/>'
        if runs and runs[-1][0] == fill:
            runs[-1][1].append(frag)
        else:
            runs.append((fill, [frag]))
    inner = "".join(f'<g fill="{f}">{"".join(fr)}</g>' for f, fr in runs)
    svg = f'<g transform="scale({n(s)})">{inner}</g>'
    return svg, (0, y0 * s, (x1 - x0) * s, y1 * s)


def wordmark(size, pal):
    """cc-lb in SemiBold: letters ink, hyphen mid, slight hyphen padding."""
    return text_g(
        wm.W600,
        brand.WORDMARK,
        size,
        lambda ch: pal["mid"] if ch == "-" else pal["ink"],
        track=TRACK,
        gap_extra={1: HYPHEN_PAD, 2: HYPHEN_PAD},  # gaps around '-'
    )


def tagline(size, pal):
    return text_g(wm.W500, brand.TAGLINE, size, lambda ch: pal["muted"])


def union(boxes):
    return (
        min(b[0] for b in boxes),
        min(b[1] for b in boxes),
        max(b[2] for b in boxes),
        max(b[3] for b in boxes),
    )


def compose(parts, pad):
    """Wrap (svg, ink_box) parts in a tight viewBox with `pad` clear space."""
    x0, y0, x1, y1 = union([b for _s, b in parts])
    body = (
        f'<g transform="translate({n(pad - x0)} {n(pad - y0)})">'
        + "".join(s for s, _b in parts)
        + "</g>"
    )
    return svg_doc(x1 - x0 + 2 * pad, y1 - y0 + 2 * pad, body)


# --- assets ------------------------------------------------------------------

def build_wordmark(pal, suffix):
    size = 60.0
    g, box = wordmark(size, pal)
    pad = round((box[3] - box[1]) * 0.14, 1)
    write(f"lockup/cc-lb-wordmark-on-{suffix}.svg", compose([(g, box)], pad))


def build_lockup_horizontal(pal, suffix):
    """Mark + wordmark; the bracket band (grid 14..50) sits on the cap band,
    the bar overshoots ~7u above and below - mark ink height ~1.39x cap."""
    size = 48.0
    s_mark = (wm.CAP_HEIGHT * (size / wm.UPM)) / 36.0
    gap = 0.50 * size  # ink-to-ink gap between mark and 'c'

    m, m_box = mark(0, 0, s_mark, pal)
    baseline = 50 * s_mark  # grid y=50 (bracket bottom) == text baseline
    g_text, t_box = wordmark(size, pal)
    tx = m_box[2] + gap
    t_box = (tx + t_box[0], baseline + t_box[1], tx + t_box[2],
             baseline + t_box[3])
    g_text = f'<g transform="translate({n(tx)} {n(baseline)})">{g_text}</g>'

    pad = round((m_box[3] - m_box[1]) * 0.14, 1)
    write(f"lockup/cc-lb-lockup-horizontal-on-{suffix}.svg",
          compose([(m, m_box), (g_text, t_box)], pad))


def build_lockup_stacked(pal, suffix):
    """Mark centred above the wordmark."""
    size = 44.0
    s_mark = 1.5  # 96px mark; brackets 54px ~ 1.23x the 30.7px cap height
    gap = 0.42 * 50 * s_mark  # ~mark ink height

    m, m_box = mark(0, 0, s_mark, pal)
    g_text, t_box = wordmark(size, pal)
    w = max(m_box[2] - m_box[0], t_box[2] - t_box[0])
    # centre both horizontally
    m_dx = (w - (m_box[2] - m_box[0])) / 2 - m_box[0]
    baseline = m_box[3] + gap - t_box[1]
    tx = (w - (t_box[2] - t_box[0])) / 2
    m = f'<g transform="translate({n(m_dx)})">{m}</g>'
    g_text = f'<g transform="translate({n(tx)} {n(baseline)})">{g_text}</g>'
    t_box = (tx, baseline + t_box[1], tx + (t_box[2] - t_box[0]),
             baseline + t_box[3])
    m_box = (m_box[0] + m_dx, m_box[1], m_box[2] + m_dx, m_box[3])

    pad = round(50 * s_mark * 0.14, 1)
    write(f"lockup/cc-lb-lockup-stacked-on-{suffix}.svg",
          compose([(m, m_box), (g_text, t_box)], pad))


def hero(pal, suffix):
    """1280x320 panel: horizontal lockup + tagline + converging-lane motif."""
    W, H = 1280.0, 320.0
    body = [
        f'<rect x="1" y="1" width="{n(W - 2)}" height="{n(H - 2)}" rx="24" '
        f'fill="{pal["panel"]}" stroke="{pal["border"]}"/>'
    ]

    size = 96.0
    s_mark = (wm.CAP_HEIGHT * (size / wm.UPM)) / 36.0
    mark_x, mark_y = 72.0, 58.0
    baseline = mark_y + 50 * s_mark
    m, _ = mark(mark_x, mark_y, s_mark, pal)
    body.append(m)
    g_text, t_box = wordmark(size, pal)
    tx = mark_x + 53 * s_mark + 0.5 * size
    body.append(f'<g transform="translate({n(tx)} {n(baseline)})">{g_text}</g>')

    tg, tg_box = tagline(33.0, pal)
    tag_x = mark_x + 11 * s_mark  # align with the mark's ink left edge
    tag_base = baseline + 66
    body.append(f'<g transform="translate({n(tag_x)} {n(tag_base)})">{tg}</g>')

    # Motif, right of the lockup: three thin lanes converge at a gate slit
    # (two stubs straddling a slot, echoing the mark); one lane runs through.
    gx = 1000.0          # gate x
    cy = baseline - 0.5 * wm.CAP_HEIGHT * (size / wm.UPM)  # optical centre
    sw = 3.0
    stub_w = 4.0
    slot = 7.0           # half-height of the open slot
    arm = 33.0           # stub length
    lanes = [
        f'<path d="M{n(W - 60)} {n(cy)}H{n(gx - 240)}" '
        f'stroke="{pal["mid"]}" stroke-width="{sw}" fill="none"/>',
    ]
    for sign in (-1, 1):
        y0 = cy + sign * 64
        y1 = cy + sign * (slot + 2)
        lanes.append(
            f'<path d="M{n(W - 60)} {n(y0)}H{n(gx + 84)}L{n(gx)} {n(y1)}" '
            f'stroke="{pal["border"]}" stroke-width="{sw}" fill="none"/>'
        )
        lanes.append(
            f'<rect x="{n(gx - stub_w / 2)}" y="{n(cy + sign * slot - (arm if sign < 0 else 0))}" '
            f'width="{n(stub_w)}" height="{n(arm)}" fill="{pal["mid"]}"/>'
        )
    body.append("<g>" + "".join(lanes) + "</g>")

    write(f"readme/cc-lb-hero-{suffix}.svg", svg_doc(W, H, "".join(body)))


def social():
    """1280x640 GitHub social preview / og:image, on DARK.

    Everything sits well inside the safe area (>40px margins all around).
    """
    pal = brand.DARK
    W, H = 1280.0, 640.0
    body = [f'<rect width="{n(W)}" height="{n(H)}" fill="{pal["bg"]}"/>']

    size = 108.0
    s_mark = (wm.CAP_HEIGHT * (size / wm.UPM)) / 36.0 * 1.28
    m, m_box = mark(0, 0, s_mark, pal)
    g_text, t_box = wordmark(size, pal)
    tg, tg_box = tagline(37.0, pal)

    gap1 = 0.42 * (m_box[3] - m_box[1])
    gap2 = 62.0  # between wordmark ink bottom and tagline ink top
    block = (m_box[3] - m_box[1]) + gap1 + (t_box[3] - t_box[1]) \
        + gap2 + (tg_box[3] - tg_box[1])
    y = (H - block) / 2

    mx = (W - (m_box[2] - m_box[0])) / 2
    body.append(f'<g transform="translate({n(mx - m_box[0])} {n(y - m_box[1])})">'
                f"{m}</g>")
    wm_y = y + (m_box[3] - m_box[1]) + gap1 - t_box[1]
    body.append(f'<g transform="translate({n((W - (t_box[2] - t_box[0])) / 2)} '
                f'{n(wm_y)})">{g_text}</g>')
    tag_y = wm_y + t_box[3] + gap2 - tg_box[1]
    body.append(f'<g transform="translate({n((W - (tg_box[2] - tg_box[0])) / 2)} '
                f'{n(tag_y)})">{tg}</g>')
    write("social/cc-lb-social-preview.svg", svg_doc(W, H, "".join(body)))


def main():
    for suffix, pal in (("dark", brand.DARK), ("light", brand.LIGHT)):
        build_wordmark(pal, suffix)
        build_lockup_horizontal(pal, suffix)
        build_lockup_stacked(pal, suffix)
        hero(pal, suffix)
    social()


if __name__ == "__main__":
    main()
