# Split Control-Plane / Data-Plane Docker Compose Stack

This Compose stack runs `cc-lb` in split three-service topology backed by PostgreSQL:

- **`proxy-a`** (`cc-lb-proxy --role proxy-worker`): serves LLM proxy traffic on port `8080` and runs Apalis background scheduler workers (`metrics`: `9091`).
- **`proxy-b`** (`cc-lb-proxy --role proxy`): serves stateless LLM proxy traffic on host port `8081` without spawning scheduler worker tasks (`metrics`: `9092`).
- **`admin`** (`cc-lb-admin`): serves the Admin API + embedded Dashboard SPA on port `9090` (`metrics`: `9093`) in PostgreSQL DB-only mode (enqueues background jobs, reads/writes PostgreSQL state directly, fans in live-tail telemetry via `PgListener` + `StorageTailPoller`, and exposes config drafting/download with `save` locked read-only).

## Quick Start

```bash
export CC_LB_MASTER_KEY="$(openssl rand -hex 32)"
export CC_LB_ADMIN_TOKEN="change-me-admin-token"

docker compose -f deploy/compose/docker-compose.split.yml up --build
```

## Building Individual Image Targets

```bash
# Default standalone monolith binary (`cc-lb`)
docker buildx build --target distroless -t cc-lb .

# Data-plane proxy/worker binary (`cc-lb-proxy`, no Admin/SPA code compiled)
docker buildx build --target proxy -t cc-lb-proxy .

# Control-plane admin binary (`cc-lb-admin`, no Proxy/Engine/Wasmtime router compiled)
docker buildx build --target admin -t cc-lb-admin .
```
