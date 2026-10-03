#!/usr/bin/env bash
# Check the wiki sources under docs/ before they are published.
#
# The wiki is flat: every page is docs/<Page-Name>.md and links between pages
# are written [Text](Page-Name). This fails when
#   - docs/Home.md is missing (the sync would publish a wiki with no front page),
#   - a page links to a page that does not exist,
#   - a file is not a markdown page (the sync only publishes *.md).
# Usage: scripts/check-docs.sh [docs-dir]
set -euo pipefail

DOCS="${1:-docs}"
status=0

[[ -f "$DOCS/Home.md" ]] || { echo "check-docs: $DOCS/Home.md is missing" >&2; exit 1; }

shopt -s nullglob
for page in "$DOCS"/*; do
  [[ -f "$page" && "$page" == *.md ]] || { echo "check-docs: $page is not a flat markdown page" >&2; status=1; }
done

for page in "$DOCS"/*.md; do
  # [text](Target) or [text](Target#anchor), skipping URLs, anchors and mailto.
  while IFS= read -r target; do
    target="${target%%#*}"
    [[ -z "$target" ]] && continue
    [[ -f "$DOCS/$target.md" ]] || { echo "check-docs: $page links to missing page '$target'" >&2; status=1; }
  done < <(grep -oE '\]\([A-Za-z0-9_-]+(#[A-Za-z0-9_-]+)?\)' "$page" | sed -E 's/^\]\(//; s/\)$//' || true)
done

exit "$status"
