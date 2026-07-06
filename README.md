# headsntails Core Engine (`headsntails-core`)

headsntails-core is a high-concurrency, lightning-fast feature flag evaluation engine written in Go. It operates on a low-latency cache model optimized for critical edge lookups.

## Data Strategy: Write-Through Cache & Hydration
To eliminate database latency bottlenecks on high-volume evaluation requests, the core engine splits its data paths:
* **The Source of Truth:** An isolated PostgreSQL instance tracks configuration schemas and transaction histories.
* **The Performance Layer:** A standalone Redis cache services execution queries in sub-milliseconds.
* **Lifecycle Flow:** On application cold start, a pipeline routine hydrates Redis entirely from PostgreSQL. Any subsequent administrative write operation performs an explicit Write-Through pattern: committing to SQL first, and updating/evicting the Redis cache block strictly upon SQL transaction success.

## API Routing Contract (v0.2)

All application routes are natively bound to the `/api/v1/` prefix.

### Public Client Endpoints (Proxied via Ingress)
* **`POST /api/v1/set`** — Administrative flag mutation. Commits payload variables to PostgreSQL and overwrites cache.
* **`POST /api/v1/get`** — Evaluates a feature flag state. Evaluates strictly out of Redis memory. *Intercepted natively by the upstream rate-limiter guard middleware.*

### Diagnostics
* **`GET /health`** — Performs deep connectivity assertions against downstream Redis and PostgreSQL clusters. (Internal container scope only).

## Environment Configuration

The engine relies entirely on runtime environment variable injection:
* `APP_ENV`: Deployment runtime environment context (`local` / `production`). Defaults to `local`.
* `APP_HOST`: The network address binding for the core application web service. Defaults to `localhost:8080`.
* `DB_HOST`: Hostname or IP address of the primary PostgreSQL transaction cluster instance. Defaults to `localhost`.
* `DB_PORT`: Port mapping allocation for the target PostgreSQL instance. Defaults to `5432`.
* `DB_USER`: Authentication user profile namespace for database connectivity. Defaults to `postgres`.
* `DB_PASS`: Authentication security string credential paired to the database user. Defaults to `postgres`.
* `DB_NAME`: Target identity database namespace target within PostgreSQL. Defaults to `headsntails`.
* `REDIS_ADDR`: Internal network endpoint address string for your standalone caching infrastructure instance. Defaults to `localhost:6379`.
* `REDIS_PASSWORD`: Optional authentication secret token string verifying access rights into the Redis node cluster.
* `REDIS_URL`: Premium TLS-secured string URL structure connection token utilized strictly when cloud provider architectures (e.g. Upstash) override default settings.
* `REDIS_HASH_KEY`: Isolated tracking key space inside your Redis dictionary layout map. Defaults to `headsntails:v1:flags`.
* `JWT_SECRET_KEY`: High-entropy symmetric validation secret key required to parse, deserialize, and verify incoming client authorization token parameters. **(Required)**
* `RATE_LIMITER_URL`: Deep routing internal microservice location pointer endpoint utilized out-of-band to track active consumer resource limit calculations. **(Required)**

## Local Development Setup

Please refer to [`headsntails` Platform](https://github.com/NGUgeneral/headsntails-platform) for detailed local development setup instructions.
