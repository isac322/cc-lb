"""Single source of the cc-lb mark ("gate"): geometry and palette.

Two squared bracket 'c' shapes face each other and form a gate; one lane
(the accent bar) passes straight through it. Every generated asset derives
from the constants here, so change the mark in this file and rebuild.
"""

# Master geometry on a 64-unit grid (mark only, no padding beyond the grid).
MASTER_GRID = 64
MASTER_LEFT = "M11 14H27V20.5H18V43.5H27V50H11Z"
MASTER_RIGHT = "M53 14H37V20.5H46V43.5H37V50H53Z"
MASTER_BAR = (29.5, 7, 5, 50)  # x, y, width, height

# Pixel-snapped variant for 16 px rendering (favicons, tiny UI).
SMALL_GRID = 16
SMALL_LEFT = "M1 3H6V5H3V11H6V13H1Z"
SMALL_RIGHT = "M15 3H10V5H13V11H10V13H15Z"
SMALL_BAR = (7, 1, 2, 14)

# Graphite palette, matching crates/cc-lb-admin/web/src/index.css.
DARK = {
    "bg": "#161719",
    "panel": "#1c1e20",
    "border": "#2f3033",
    "ink": "#edeeee",
    "accent": "#ccddee",
    "mid": "#6a6c6f",
    "muted": "#c0c1c3",
}
LIGHT = {
    "bg": "#e3e5e7",
    "panel": "#eeeff1",
    "border": "#cacccf",
    "ink": "#18191c",
    "accent": "#1f2c3d",
    "mid": "#7e8084",
    "muted": "#484a4e",
}
MONO_BLACK = "#000000"
MONO_WHITE = "#ffffff"

WORDMARK = "cc-lb"
TAGLINE = "Anthropic-compatible load balancer for Claude Code"
