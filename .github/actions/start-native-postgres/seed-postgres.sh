#!/usr/bin/env bash
set -euo pipefail

write_failed() {
  local rc=$?
  printf 'start-native-postgres failed with exit code %d\n' "$rc" > "${FAIL_FILE}.tmp" 2>/dev/null || true
  mv -f "${FAIL_FILE}.tmp" "$FAIL_FILE" 2>/dev/null || true
}
trap write_failed ERR

seed_sched_migrations() {
  local db="$1"
  local url="postgres://postgres:testpw@127.0.0.1:5432/${db}"
  # Run apalis migrations under default search_path=public so `hstore` installs
  # into `public` (visible to both uneditable default-search_path unit tests
  # and search_path=cc_lb_scheduler,apalis,public runtime connections).
  sqlx migrate run \
    --config "${APALIS_DIR}/sqlx.toml" \
    --source "${APALIS_DIR}/migrations" \
    --database-url "$url"
  {
    printf 'CREATE SCHEMA IF NOT EXISTS cc_lb_scheduler;\n'
    printf 'SET search_path = cc_lb_scheduler, apalis, public;\n'
    while IFS= read -r mig; do
      cat "$mig"
      printf '\n;\n'
    done < <(LC_ALL=C ls -1 crates/cc-lb-scheduler/migrations/postgres/*.sql)
  } | psql -h 127.0.0.1 -p 5432 -U postgres -d "$db" -v ON_ERROR_STOP=1 -f -

  psql -h 127.0.0.1 -p 5432 -U postgres -d postgres -v ON_ERROR_STOP=1 <<SQL
UPDATE pg_database SET datistemplate = true WHERE datname = '${db}';
CREATE DATABASE ${db}_0 TEMPLATE ${db};
CREATE DATABASE ${db}_1 TEMPLATE ${db};
CREATE DATABASE ${db}_2 TEMPLATE ${db};
CREATE DATABASE ${db}_3 TEMPLATE ${db};
CREATE DATABASE ${db}_4 TEMPLATE ${db};
CREATE DATABASE ${db}_5 TEMPLATE ${db};
CREATE DATABASE ${db}_6 TEMPLATE ${db};
CREATE DATABASE ${db}_7 TEMPLATE ${db};
UPDATE pg_database SET datistemplate = true WHERE datname LIKE '${db}_%';
SQL
}

pgdata="/dev/shm/pgdata"
rm -rf "$pgdata"
mkdir -p "$pgdata"
chmod 700 "$pgdata"

"$PG_BIN/initdb" -D "$pgdata" -U postgres --auth=trust --encoding=UTF8 --locale=C.UTF-8 --no-sync
"$PG_BIN/pg_ctl" -D "$pgdata" -l /dev/shm/postgres.log \
  -o "-k /dev/shm -p 5432 -c fsync=off -c synchronous_commit=off -c full_page_writes=off -c autovacuum=off -c wal_level=minimal -c max_wal_senders=0 -c archive_mode=off -c jit=off -c bgwriter_lru_maxpages=0 -c work_mem=32MB -c maintenance_work_mem=128MB -c max_wal_size=2GB -c max_connections=300 -c shared_buffers=256MB -c checkpoint_timeout=1h" \
  start

ready=0
for _ in $(seq 1 120); do
  if pg_isready -h 127.0.0.1 -p 5432 -U postgres >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 0.05
done
if [ "$ready" -ne 1 ]; then
  echo "PostgreSQL failed to become ready within timeout" >&2
  exit 1
fi
pg_isready -h 127.0.0.1 -p 5432 -U postgres

psql -h 127.0.0.1 -p 5432 -U postgres -d template1 -v ON_ERROR_STOP=1 -c "CREATE EXTENSION IF NOT EXISTS pgcrypto;"
psql -h 127.0.0.1 -p 5432 -U postgres -d postgres -v ON_ERROR_STOP=1 <<'SQL'
ALTER USER postgres WITH PASSWORD 'testpw';
CREATE DATABASE cc_lb_app_template;
SQL

app_template_url="postgres://postgres:testpw@127.0.0.1:5432/cc_lb_app_template"
sqlx migrate run --source crates/cc-lb-storage-postgres/migrations --database-url "$app_template_url"
psql -h 127.0.0.1 -p 5432 -U postgres -d cc_lb_app_template -v ON_ERROR_STOP=1 \
  -c "INSERT INTO meta (key, value) VALUES ('backend_kind', 'postgres') ON CONFLICT (key) DO NOTHING;"

psql -h 127.0.0.1 -p 5432 -U postgres -d postgres -v ON_ERROR_STOP=1 <<'SQL'
UPDATE pg_database SET datistemplate = true WHERE datname = 'cc_lb_app_template';
CREATE DATABASE cc_lb_test TEMPLATE cc_lb_app_template;
CREATE DATABASE cc_lb_app_template_0 TEMPLATE cc_lb_app_template;
CREATE DATABASE cc_lb_app_template_1 TEMPLATE cc_lb_app_template;
CREATE DATABASE cc_lb_app_template_2 TEMPLATE cc_lb_app_template;
CREATE DATABASE cc_lb_app_template_3 TEMPLATE cc_lb_app_template;
CREATE DATABASE cc_lb_app_template_4 TEMPLATE cc_lb_app_template;
CREATE DATABASE cc_lb_app_template_5 TEMPLATE cc_lb_app_template;
CREATE DATABASE cc_lb_app_template_6 TEMPLATE cc_lb_app_template;
CREATE DATABASE cc_lb_app_template_7 TEMPLATE cc_lb_app_template;
UPDATE pg_database SET datistemplate = true WHERE datname LIKE 'cc_lb_app_template_%';
SQL

if [ "${SEED_SCHED:-false}" = "true" ] && [ -n "${APALIS_DIR:-}" ] && [ -d "${APALIS_DIR}/migrations" ] && [ -f "${APALIS_DIR}/sqlx.toml" ]; then
  # Keep cc_lb_sched_template free of app tables (apalis_housekeeping unit
  # tests create cache_keepalive_sessions/evidence/meta without IF NOT EXISTS),
  # and separately provide cc_lb_full_template with both app + scheduler.
  psql -h 127.0.0.1 -p 5432 -U postgres -d postgres -v ON_ERROR_STOP=1 <<'SQL'
CREATE DATABASE cc_lb_sched_template;
CREATE DATABASE cc_lb_full_template TEMPLATE cc_lb_app_template;
SQL
  seed_sched_migrations cc_lb_sched_template
  seed_sched_migrations cc_lb_full_template
fi

: > "${READY_FILE}.tmp"
mv -f "${READY_FILE}.tmp" "$READY_FILE"
