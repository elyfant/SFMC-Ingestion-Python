#!/usr/bin/env bash
# Bridges SFMC-Ingestion-Python's downloaded files into
# Inkfish-glider-data-pipeline's data/ folder via symlinks - no copying,
# so there's nothing to keep in sync and no extra disk usage.
#
# Safe to re-run: skips a glider if its symlink already points at the
# right place, and refuses to touch anything that isn't already a
# symlink we created (so it won't clobber a real directory by accident).
#
# Usage: ./setup_symlinks.sh
# Edit the two paths below once per machine, then run it once per new
# glider added to GLIDERS.

set -euo pipefail

RT_DATA_ROOT="$HOME/data/rt-data"
INKFISH_DATA_DIR="$HOME/glider-app/Inkfish-glider-data-pipeline/data"

GLIDERS=("selkie" "unit_1272")

mkdir -p "$INKFISH_DATA_DIR"

for glider in "${GLIDERS[@]}"; do
  src="$RT_DATA_ROOT/$glider/from-glider"
  link="$INKFISH_DATA_DIR/rt-$glider"

  if [ ! -d "$src" ]; then
    echo "SKIP  $glider: $src doesn't exist yet (has SFMC-Ingestion-Python synced it at least once?)"
    continue
  fi

  if [ -L "$link" ]; then
    current_target="$(readlink -f "$link")"
    if [ "$current_target" = "$(readlink -f "$src")" ]; then
      echo "OK    $glider: $link already points at $src"
      continue
    else
      echo "FIX   $glider: $link points at $current_target, relinking to $src"
      ln -sfn "$src" "$link"
      continue
    fi
  fi

  if [ -e "$link" ]; then
    echo "SKIP  $glider: $link exists and is NOT a symlink - not touching it." \
         "Move it aside manually if you want the symlink here."
    continue
  fi

  ln -s "$src" "$link"
  echo "LINK  $glider: created $link -> $src"
done
