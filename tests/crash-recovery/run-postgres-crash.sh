#!/usr/bin/env bash
set -euo pipefail

: "${CI_POSTGRES_URL:?CI_POSTGRES_URL must be set}"

cargo test -p cc-lb-storage-postgres --test crash_recovery --features postgres -- --nocapture
echo "Postgres crash recovery: PASS"
