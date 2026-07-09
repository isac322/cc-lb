#!/usr/bin/env bash
set -eu

BAD=$(rg -nF 'refresh_token' crates/cc-lb-engine/src crates/cc-lb-admin/src crates/cc-lb-server/src crates/cc-lb-signer-anthropic-oauth/src 2>/dev/null | rg -v 'tests/' | rg '(tracing::|audit_sink|info!|error!|warn!|debug!|trace!|format!|println!)' || true)

if [ -n "$BAD" ]; then
  echo "Forbidden refresh_token literal near logging/audit:"
  echo "$BAD"
  exit 1
fi
