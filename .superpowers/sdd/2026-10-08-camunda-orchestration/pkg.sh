#!/usr/bin/env bash
# Review package = diff dvou snapshotů: pkg.sh FROM TO
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
W=.superpowers/sdd/2026-10-08-camunda-orchestration
out="$W/review-$1..$2.diff"
{ echo "# Review package: snapshot $1 -> $2 (uncommitted working tree; user commits manually)"; echo; echo "## Files changed";
  git -c core.autocrlf=false -c core.safecrlf=false diff --no-index --stat "$W/snap-$1" "$W/snap-$2" || true; echo; echo "## Diff";
  git -c core.autocrlf=false -c core.safecrlf=false diff --no-index -U10 "$W/snap-$1" "$W/snap-$2" | sed "s#$W/snap-$1/##g; s#$W/snap-$2/##g" || true; } > "$out" 2>/dev/null
echo "wrote $out: $(wc -c < "$out") bytes"
