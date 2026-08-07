# Database API Gateway — Design

Status: approved 2026-08-06, not yet implemented.

## Problem

Every member of the fund connects to Postgres with the same shared credential. That
means:

- **No attribution.** When data changes, the logs say `fund_member` or `postgres`. There
  is no way to know who did it.
- **No per-person control.** Access is granted by handing out a password. It cannot be
  revoked from one person without changing it for everyone.
- **No resource control.** A single unbounded query against `sharadar_ohlcv_1d`
  (45.5M rows) can exhaust a server that has 957MB of RAM and is already deep in swap.

Reads through pgAdmin are already constrained by the read-only `fund_member` Postgres
role, so data is not currently at risk of accidental modification. What is missing is a
way to write in a controlled, attributable way, and any visibility into who does what.

## Goals

1. Every database edit is attributable to a named person and recorded.
2. Access is granted per person and revocable per person.
3. Requests are bounded so no single caller can exhaust the server.
4. Adding and removing people is a one-row operation.

## Non-goals

- **Logging reads made through pgAdmin.** Those bypass the service entirely and cannot
  be attributed, because everyone connects with one of two shared logins. **This is a
  settled decision, not a limitation awaiting a fix:** per-person database logins were
  considered and rejected. Reads made *through the API* are logged; reads made in pgAdmin
  are not, permanently.
- **Auditing admin writes made outside the API.** The six admins share the `postgres`
  login by decision, so an admin editing in pgAdmin appears in Postgres's own logs as
  `postgres`, with no name attached. The audit log answers "what changed through the
  API", not "what changed". Closing this would require individual admin logins.
- **Replacing pgAdmin.** It remains the read tool. The API is additive.

## Architecture

One new FastAPI service between users and Postgres. Users hold an API key; they never
hold a database credential.

**The database performs the enforcement, not the service.** For each request the service
authenticates the caller, switches the connection to that caller's Postgres role with
`SET ROLE`, and runs their SQL. Postgres accepts or rejects it according to that role's
grants.

### Why the database and not the service

The service cannot determine what a SQL statement writes to without parsing it, and
parsing it correctly is not feasible. The target can be concealed in a comment, buried in
a CTE, split across statements, hidden inside a `DO` block, or left unqualified so that
it resolves through `search_path`:

```sql
INSERT INTO /* research.notes */ equities_data.ohlcv_1d VALUES (...);
WITH x AS (INSERT INTO research.a VALUES (1) RETURNING *)
     INSERT INTO equities_data.ohlcv_1d SELECT * FROM x;
DO $$ BEGIN EXECUTE 'DELETE FROM backtest.results'; END $$;
```

Handling these correctly requires a real SQL parser plus `search_path` resolution plus
following functions and triggers, and every gap is a permission bypass. Postgres already
has a parser and a permission system; running the statement as the caller delegates the
problem to the component that solves it correctly.

This also means raw SQL is safe for writes as well as reads, which keeps the API
consistent with what people already do in pgAdmin.

### Request flow

```
1. Extract API key from the Authorization header
2. sha256(key) -> look up auth.api_keys -> email, name, role, caps
   no match / inactive -> 401, logged
3. Acquire a concurrency slot: per-user cap, then global cap
   none available -> 429, logged
4. SET ROLE <the caller's role>
5. Execute their SQL
   role lacks permission -> Postgres raises, -> 403, logged
6. RESET ROLE, write an audit row, release the slot, return the result
```

Every outcome produces an audit row, including authentication failures, permission
denials and rate limits. A log recording only successes says nothing about someone
probing for access.

## The three roles

| Role | Assigned to | Reads | Writes |
|---|---|---|---|
| `db_readonly` | General members | All data schemas | Nothing |
| `db_readwrite` | Quant dev | All data schemas | Everything except `trading` and `auth` |
| `db_readwrite_all` | Admin | Everything | Everything |

Quant dev can write `equities_data`, `futures_data`, `backtest`, `research`,
`synthetic`, `macro_data`, `eia` and `metadata`. Requiring an admin to backfill a few
rows would add days of turnaround to routine work; the audit log is what makes the looser
grant acceptable, since a bad write is now attributable and recoverable from backups.

Two carve-outs:

- **`trading`** holds live positions, executions and results. A wrong backfill in
  `equities_data` produces bad research that can be re-derived. A wrong `UPDATE` on
  `trading.positions` misstates what the fund holds. Different failure mode, and nobody
  is blocked by being unable to hand-edit live positions.
- **`auth`** holds the key table and the audit log. Write access there would let someone
  grant themselves admin or delete their own trail, defeating the system. **This is what
  keeps the log itself out of reach** — the audit table is not a separate carve-out, it
  sits inside `auth` precisely so that no rule has to be remembered for it.

**Exceptions are additional roles, not a separate mechanism.** Because Postgres performs
the enforcement, a service-level exception cannot grant what a role lacks — Postgres
refuses regardless. If someone needs a permission set none of the three cover, that is a
fourth role with those grants, and they are assigned to it. If a permission set is worth
granting, it is worth naming.

Role definitions live in a SQL migration, so they are version-controlled, reviewable and
attributable. There is no separate permissions file to keep in sync.

## Components

### `auth.api_keys`

Lives in `auth`, which is already excluded from the `fund_member` and `quant_dev` grants
— admin-only by default rather than by remembering to lock it down.

```sql
CREATE TABLE auth.api_keys (
    email            TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    db_role          TEXT NOT NULL
        CHECK (db_role IN ('db_readonly','db_readwrite','db_readwrite_all')),

    key_hash         TEXT NOT NULL UNIQUE,   -- sha256 hex; the key itself is never stored
    key_prefix       TEXT NOT NULL,          -- e.g. 'ag_qd_7Kx9', for display only

    max_concurrent       INT NOT NULL DEFAULT 1,
    statement_timeout_ms INT NOT NULL DEFAULT 120000,

    active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by       TEXT,
    last_used_at     TIMESTAMPTZ,
    revoked_at       TIMESTAMPTZ
);
```

Email is the primary key: one row per person, and the table is the complete picture of
who has access. Adding someone is one `INSERT`; removing them is `active = false`.

Anything added later that references a person should carry `ON DELETE CASCADE`, so
removing a row cannot leave orphans.

**Soft delete, not hard.** Keeping the row preserves the answer to "who had access in
March?"

**The `CHECK` constraint is load-bearing.** Without it a typo produces a row naming a role
that does not exist, and the failure surfaces at request time rather than at write time.

**No Postgres role is created per person.** Three roles serve everyone; the table maps
people onto them. This is what keeps offboarding to a single update — per-person roles
would mean `DROP ROLE` failing whenever that person owns a table, and reassigning owned
objects before they can be removed.

### Key format and lifecycle

```
ag_qd_7Kx9mPw2nR4vT8yB3cF6hJ1sL5dG0aZq
└┬┘ └┬┘ └──────────── secrets.token_urlsafe(24) ────────────┘
 │   └── role prefix (ro / rw / ad)
 └────── org prefix: greppable in leaked code, teachable to secret scanners
```

Only `sha256(key)` is stored; the plaintext is displayed once, at creation.

SHA-256 rather than bcrypt: bcrypt exists to slow brute-forcing of guessable secrets such
as human-chosen passwords. A 32-character random token has enough entropy that brute
force is irrelevant, and SHA-256 is fast enough to run on every request.

This matters concretely here. This database was internet-facing with a published password
for months during 2026. If that recurs and the key table is plaintext, every key is
immediately usable; hashed, the leak is worthless.

| Action | Mechanism |
|---|---|
| Create | Admin CLI script: generates key, inserts hash, prints plaintext once |
| Rotate | Same script; overwrites `key_hash`, old key stops working immediately |
| Revoke | `active = false`, `revoked_at = now()` |

**Key creation is a CLI script, not an endpoint**, to avoid a bootstrap problem: an
endpoint would require a key in order to create the first key.

### Endpoints

```
POST /v1/query      { "sql": "..." }     -- any SQL; the role decides what is permitted

GET  /v1/schemas
GET  /v1/tables?schema=...
GET  /v1/columns?schema=...&table=...
```

One execution endpoint, because the role determines what the statement may do. There is
no read/write distinction in the API surface — that distinction is the database's.

The metadata endpoints exist so tooling can discover structure programmatically rather
than a person reading it out of pgAdmin. They are convenience wrappers over
`information_schema`, which Postgres already filters to what the calling role may see, so
they need no permission logic of their own.

### `auth.audit_log`

```sql
CREATE TABLE auth.audit_log (
    id             BIGSERIAL PRIMARY KEY,
    occurred_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    actor_email    TEXT NOT NULL,
    actor_name     TEXT NOT NULL,
    actor_role     TEXT NOT NULL,
    key_prefix     TEXT,

    statement      TEXT,
    row_count      INTEGER,

    outcome        TEXT NOT NULL,   -- success | denied | error | rate_limited
    error_message  TEXT,
    duration_ms    INTEGER,
    client_ip      INET
);

CREATE INDEX ON auth.audit_log (occurred_at DESC);
CREATE INDEX ON auth.audit_log (actor_email, occurred_at DESC);
```

**Actor identity is denormalised deliberately.** Storing only the email and joining to
`api_keys` for the name would erase a person's name from every historical entry when they
are removed — the trail degrades as people leave, which is backwards. Storing only the
name loses the stable identifier, since names are neither unique nor permanent.
Snapshotting both makes each row self-contained, historically accurate and readable
without a join.

The statement is stored verbatim. Since the role decides what was permitted, the SQL text
is the record of what was attempted, whether or not it succeeded.

Lives in `auth`, so nobody can read or delete their own activity.

## Resource limits

The server has 957MB of RAM, roughly 330MB available, and over 1GB in swap while running
Airflow, Postgres, promtail and trade-ngin.

| Setting | Value | Rationale |
|---|---|---|
| Container memory limit | 200MB | Chosen so the API is what dies under pressure |
| Global concurrency | 2 | Deliberately pessimistic; raise after observing real usage |
| Per-user concurrency | 1 (per-user override) | Twenty users at one request each still overwhelms this box |
| Read row limit | 100,000 | `SELECT * FROM sharadar_ohlcv_1d` is 45.5M rows |
| Statement timeout | 120s (per-user override) | Catches queries that never produce a first row |

**The container memory limit is the most important line here.** Without it, if the API
balloons, the kernel OOM killer selects a victim by its own scoring, and the largest
memory consumer on that box is often Postgres — the database could be lost to an API bug.
With a hard cap, Docker kills the API container instead and Postgres never notices. The
point is choosing which thing fails.

The row limit is the most likely day-one outage prevented: a single unbounded `SELECT *`
would try to materialise 45.5M rows inside a 200MB container.

120 seconds rather than something tighter because legitimate aggregations over 45M rows
on a swapping box are genuinely slow, and a timeout that kills real work teaches people
to route around the API — which defeats the logging.

## Deployment

The service runs as an additional container on the existing host, bound to `localhost`
only, with Caddy in front of it terminating TLS.

```
data-ngin.algogators.com {
    reverse_proxy localhost:8000
}
```

**TLS is required, not optional.** Callers transmit API keys in a header; over plain HTTP
those are readable by anyone on the network path. The certificate is not about the keys —
it proves the server's identity and encrypts the connection in both directions.

Certificate authorities issue certificates for domain names, never for bare IP addresses,
which is the only reason a subdomain is involved. The alternative is a self-signed
certificate, which encrypts but makes every client show a warning, so everyone learns to
disable certificate verification — a worse habit than the problem it solves.

Caddy rather than nginx with certbot because certbot renewal is a scheduled job that can
fail silently. This deployment already lost nine months of database backups to exactly
that failure mode: a nightly cron job that ran, failed, and uploaded empty files without
anyone noticing. Caddy obtains and renews certificates itself, which removes that class
of failure for roughly 10MB more memory.

**Prerequisites, in order:**

1. **An Elastic IP**, attached. Without one the instance's public address changes on
   stop/start — which happens when the instance is resized — and the DNS record breaks.
   (An Elastic IP was allocated and released on 2026-08-06; allocate a fresh one at this
   point. AWS bills every public IPv4 since February 2024, so an attached Elastic IP
   costs the same as the auto-assigned address it replaces.)
2. **A DNS A record** for `data-ngin.algogators.com` pointing at it. The domain is on
   GitHub Pages; this is an independent record and no traffic passes through the site.
3. **Caddy**, then the service.

**Running on the existing host is a budget constraint, not the right architecture.** A
separate instance (~$8/month) would isolate API failures from ingestion and keep the write
credential on a different machine from the database. No spend is available. When the
instance is upgraded — already required for the Airflow 3 migration in PR #42 —
relocating this service is the first thing to revisit. The limits above are tight because
of the hardware, not because they are correct on merit.

## What is cheap to change later, and what is not

**Cheap** — a config value or one line plus a restart: memory limit, both concurrency
caps, row limit, statement timeout, and the grants attached to any role. Moving to a
larger instance changes nothing about the design.

**Sticky** — needs a migration or breaks existing clients: the `api_keys` and `audit_log`
schemas, the key format once keys are issued, and the decision to enforce through Postgres
roles rather than in the service.

## Deferred

- **Audit log retention.** The table grows without bound. Low volume makes this slow, but
  a year of daily use is real. Add a retention job later; it should be a known task
  rather than a surprise.
- **Rejecting oversized requests before execution.** The row limit truncates after the
  fact; estimating cost up front via `EXPLAIN` would reject earlier.
- **Per-schema read permissions.** All three roles currently read everything outside
  `auth`. Narrowing this means additional roles.
- **Airflow behind the same Caddy instance.** The Airflow UI on port 8080 is plain HTTP
  and open to the internet, so its logins travel in cleartext. Adding
  `airflow.algogators.com` to the Caddy config and closing 8080 would retire that. Out of
  scope here, but it is two additional lines once Caddy exists.
