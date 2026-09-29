#!/usr/bin/env bash
# Build every cc-lb brand asset and refresh the admin-web public copies.
# Runnable from anywhere:  bash assets/brand/tools/build_all.sh
set -euo pipefail
export PYTHONDONTWRITEBYTECODE=1

TOOLS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BRAND="$(cd "$TOOLS/.." && pwd)"
ROOT="$(cd "$BRAND/../.." && pwd)"
RENDER="$TOOLS/render_png.mjs"
PUBLIC="$ROOT/crates/cc-lb-admin/web/public"

SCRATCH="$(mktemp -d)"
trap 'rm -rf "$SCRATCH"' EXIT

echo "== marks + svg =="
python3 "$TOOLS/build_marks.py" --scratch-tiles "$SCRATCH"

TILE_DARK="$BRAND/app-icon/cc-lb-app-icon-dark.svg"
TILE_MASKABLE="$BRAND/app-icon/cc-lb-app-icon-maskable.svg"

echo "== app-icon png =="
# 16/32 px render from integer-snapped tiles: the canvas is the output size
# and the 16-grid mark is drawn at integer scale (1x at 16, 2x at 32), so
# every mark edge lands on a pixel boundary. Larger sizes use the master tile.
for n in 16 32; do
  node "$RENDER" "$SCRATCH/tile-$n.svg" \
    "$BRAND/app-icon/png/cc-lb-app-icon-$n.png" "$n" "$n"
done
for n in 48 64 128 180 192 256 512; do
  node "$RENDER" "$TILE_DARK" \
    "$BRAND/app-icon/png/cc-lb-app-icon-$n.png" "$n" "$n"
done
for n in 192 512; do
  node "$RENDER" "$TILE_MASKABLE" \
    "$BRAND/app-icon/png/cc-lb-app-icon-maskable-$n.png" "$n" "$n"
done

echo "== favicon =="
# apple-touch-icon: opaque square, DARK bg, no rounded corners (iOS rounds it).
# The maskable layout already keeps the mark inside the safe zone, so the
# same composition survives iOS corner masking.
node "$RENDER" "$TILE_MASKABLE" "$BRAND/favicon/apple-touch-icon.png" 180 180
python3 "$TOOLS/build_ico.py"

echo "== web/public =="
mkdir -p "$PUBLIC"
cp "$BRAND/favicon/favicon.svg"       "$PUBLIC/favicon.svg"
cp "$BRAND/favicon/favicon.ico"       "$PUBLIC/favicon.ico"
cp "$BRAND/favicon/apple-touch-icon.png" "$PUBLIC/apple-touch-icon.png"

# ---------------------------------------------------------------------------
echo "== compositions =="
python3 "$TOOLS/build_compositions.py"
node "$RENDER" "$BRAND/social/cc-lb-social-preview.svg" \
  "$BRAND/social/cc-lb-social-preview.png" 1280 640
# ---------------------------------------------------------------------------

echo "done."
