#!/usr/bin/env bash
# Snapshot pracovního stromu (tracked + untracked, bez ignorovaných) do $W/snap-<name>; bez git zápisů.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
W=.superpowers/sdd/2026-10-08-camunda-orchestration
dest="$W/snap-$1"; rm -rf "$dest"; mkdir -p "$dest"
git ls-files -co --exclude-standard | grep -v "^docs/superpowers/" | while IFS= read -r f; do [ -f "$f" ] && cp --parents "$f" "$dest"; done; true
echo "snapshot $dest: $(find "$dest" -type f | wc -l) files"
