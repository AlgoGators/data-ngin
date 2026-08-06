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
  (45.5M rows) can take down a server that has 957MB of RAM and is already ~1.3GB into
  swap.

Reads through pgAdmin are already constrained by the read-only `fund_member` Postgres
role, so the data is not currently at risk of accidental modification. What is missing
is the ability to write in a controlled, attributable way, and any visibility into who
is doing what.

## Goals

1. Every database *edit* is attributable to a named person and recorded.
2. Permissions are per-person, not only per-role. Nobody can widen their own access.
3. Requests are bounded so no single caller can exhaust the server.
4. Adding and removing people is a one-row operation.

## Non-goals

- **Logging reads made through pgAdmin.** Those bypass the service entirely and cannot
  be attributed while pgAdmin uses a shared login. Reads made *through the API* are
  logged; that coverage improves as people adopt the API. Complete read logging would
  require per-person Postgres accounts and removing direct access — a separate,
  later decision.
- **Auditing admin writes made outside the API.** Admins hold Postgres write access, so
  an admin editing in pgAdmin will not appear in the audit log. The log answers "what
  changed through the API", not "what changed". Closing this would mean removing direct
  write access from admins.
- **Replacing pgAdmin.** It remains the read tool. The API is additive.

## Architecture

One new FastAPI service between users and Postgres. It holds two connections and users
hold neither:

| Connection | Postgres role | Used for |
|---|---|---|
| Read | `fund_member` (read-only) | Every read |
| Write | `api_writer` (new) | Writes, only after a permission check passes |

Users authenticate with an API key. They never receive a database credential capable of
writing.

`api_writer` needs write grants on every schema the API may write to — that is, the union
of what any tier can reach, including the protected schemas that only admins may target.
It is therefore the most privileged credential in the system and lives only in the
service's configuration.

**A consequence worth stating plainly:** because there is a single write role, the
read/write split is defended twice but the *scope* of a write is defended once. A bug in
the gate cannot turn a read into a write — the read-only role refuses that — but it could
in principle let a quant_dev write somewhere only an admin should. Splitting `api_writer`
into per-tier roles would close this; see Deferred.

### Request flow

```
1. Extract API key from the Authorization header
2. sha256(key) -> look up auth.api_keys -> email, name, tier, exceptions, caps
   no match / inactive -> 401, logged
3. Acquire a concurrency slot: per-user cap, then global cap
   none available -> 429, logged
4. Check permission: tier permissions + user exceptions
   not permitted -> 403, logged
5. Execute on the appropriate connection
6. Write an audit row: who, when, operation, target, statement, outcome, duration
7. Release the slot, return the result
```

### Two properties this design depends on

**The permission check and the connection choice are independent defenses.** If the gate
had a bug and admitted a write on a read path, the read-only Postgres role would still
refuse it. Enforcement of the read/write boundary does not rest on the application logic
being correct. (Enforcement of *which schema* a write targets does — see the note on
`api_writer` above.)

**The audit row is written regardless of outcome**, including authentication failures,
permission denials and rate limits. A log that records only successes says nothing about
someone probing for access.

## Components

### `auth.api_keys`

Lives in `auth` because that schema is already excluded from the `fund_member` and
`quant_dev` Postgres grants — admin-only by default rather than by remembering to lock
it down.

```sql
CREATE TABLE auth.api_keys (
    email            TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    tier             TEXT NOT NULL CHECK (tier IN ('general_member','quant_dev','admin')),

    key_hash         TEXT NOT NULL UNIQUE,   -- sha256 hex; the key itself is never stored
    key_prefix       TEXT NOT NULL,          -- e.g. 'ag_qd_7Kx9', for display only

    permission_exceptions TEXT[] NOT NULL DEFAULT '{}',
    max_concurrent   INT NOT NULL DEFAULT 1,
    statement_timeout_ms INT NOT NULL DEFAULT 120000,

    active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by       TEXT,
    last_used_at     TIMESTAMPTZ,
    revoked_at       TIMESTAMPTZ
);
```

Email is the primary key: one row per person. Adding someone is one `INSERT`; removing
them is `active = false`.

**Soft delete, not hard.** Keeping the row preserves the answer to "who had access in
March?" A deleted row cannot answer that.

**The `CHECK` on tier is load-bearing.** Without it, a typo such as `quantdev` creates a
user whose tier does not exist in the permissions file, and behaviour then depends on how
the lookup was written. The constraint makes that state unrepresentable.

### Key format and lifecycle

```
ag_qd_7Kx9mPw2nR4vT8yB3cF6hJ1sL5dG0aZq
└┬┘ └┬┘ └──────────── secrets.token_urlsafe(24) ────────────┘
 │   └── tier prefix (gm / qd / ad)
 └────── org prefix: greppable in leaked code, teachable to secret scanners
```

Only `sha256(key)` is stored. The plaintext is displayed once, at creation.

SHA-256 rather than bcrypt: bcrypt exists to slow brute-forcing of *guessable* secrets
such as human-chosen passwords. A 32-character random token has enough entropy that
brute force is irrelevant, and SHA-256 is fast enough to run on every request.

This matters concretely here. This database was internet-facing with a published
password for months during 2026. If that recurs and the key table is plaintext, every
key is immediately usable; hashed, the leak is worthless.

| Action | Mechanism |
|---|---|
| Create | Admin CLI script: generates key, inserts hash, prints plaintext once |
| Rotate | Same script; overwrites `key_hash`, old key stops working immediately |
| Revoke | `active = false`, `revoked_at = now()` |

**Key creation is a CLI script, not an endpoint**, to avoid a bootstrap problem: an
endpoint would require a key in order to create the first key.

### Permissions

Two layers, deliberately stored in different places.

**Tier definitions live in a JSON file in the repo**, so changing what an entire tier can
do goes through a pull request and acquires review, history and blame.

```json
{
  "version": 1,
  "roles": {
    "general_member": { "permissions": ["read"] },
    "quant_dev": {
      "permissions": ["read", "*:research", "*:synthetic",
                      "*:macro_data", "*:eia", "*:metadata"]
    },
    "admin": { "permissions": ["*"] }
  }
}
```

**Per-user exceptions live in `api_keys.permission_exceptions`**, because they change
often and per-person. A researcher who needs write access to one schema receives an
exception rather than a promotion. Only admins can edit that column, so nobody can widen
their own access.

```
effective_permissions = tier.permissions + user.permission_exceptions
```

Grammar is `verb:schema`, with `*` permitted on either side. Verbs: `insert`, `update`,
`delete`, `create_table`, `drop_table`. `read` is a bare permission with no schema.

An unknown tier fails closed: denied and logged, never defaulted.

**Reads are all-or-nothing in the JSON.** Because reads are raw SQL, the gate cannot know
which schemas a query touches without parsing SQL. `read` grants use of the read
endpoint; *what is readable* is enforced by the read-only Postgres role's grants, exactly
as in pgAdmin today. Narrowing reads per-schema would require a second read-only Postgres
role, not a JSON change.

### Endpoints

Reads are raw SQL; writes are structured. The asymmetry is deliberate.

```
POST /v1/query          { "sql": "SELECT ..." }

POST /v1/insert         { "schema": ..., "table": ..., "rows": [...] }
POST /v1/update         { "schema": ..., "table": ..., "set": {...}, "where": "..." }
POST /v1/delete         { "schema": ..., "table": ..., "where": "..." }
POST /v1/create_table   { "schema": ..., "table": ..., "columns": [...] }
POST /v1/drop_table     { "schema": ..., "table": ... }
```

**Why writes are not raw SQL.** To permission a write, the gate must know its target
schema. With raw SQL that means either parsing it — rejected as a rabbit hole with an
unbounded set of edge cases — or trusting a caller-supplied declaration, which is
spoofable. Structured requests make the target a field rather than an inference, so the
check is exact.

Reads do not have this problem because they do not need per-schema checks; the read-only
role already bounds them.

`where` remains a raw SQL fragment. Structured, equality-only predicates would be too
limiting, and since the caller is already authorised to write that table, a free-form
predicate grants nothing extra.

**Accepted limitation:** genuinely complex writes — `UPDATE ... FROM` with a join,
multi-statement transactions, bulk `INSERT ... SELECT` — do not fit these endpoints and
remain admin-in-pgAdmin operations. Revisit if that proves common.

### `auth.audit_log`

```sql
CREATE TABLE auth.audit_log (
    id             BIGSERIAL PRIMARY KEY,
    occurred_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    actor_email    TEXT NOT NULL,
    actor_name     TEXT NOT NULL,
    actor_tier     TEXT NOT NULL,
    key_prefix     TEXT,

    operation      TEXT NOT NULL,
    target_schema  TEXT,
    target_table   TEXT,
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

**Actor identity is denormalised on purpose.** Storing only the email and joining to
`api_keys` for the name means removing someone erases their name from every historical
entry — the audit trail would degrade as people leave, which is backwards. Storing only
the name loses the stable identifier: names are not unique and do change. Snapshotting
both makes each row self-contained, historically accurate, and readable without a join.

Lives in `auth`, so it inherits admin-only access: nobody can read or delete their own
activity.

## Resource limits

The server has 957MB of RAM, roughly 333MB available, and about 1.3GB in swap while
running Airflow, Postgres, promtail and trade-ngin.

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

## Deployment

The service runs as a fourth container on the existing host.

**This is a budget-constrained decision, not the right architecture.** A separate
instance (~$8/month) would isolate API failures from ingestion and keep the write
credential on a different machine from the database. That was ruled out because no spend
is available. When the instance upgrade happens — already required for the Airflow 3
migration in PR #42 — relocating this service is the first thing to revisit. The limits
above are tight because of the hardware, not because they are correct on merit.

**TLS is required, not optional.** Callers transmit API keys in a header; over plain HTTP
those are readable by anyone on the network path. A service whose purpose is
authentication cannot ship without it. Port 443 is already open in security group
`sg-046d7aeb9acb4f883`; the service needs either a reverse proxy terminating TLS or a
certificate of its own.

## What is cheap to change later, and what is not

**Cheap** — a config value or one line plus a restart: memory limit, both concurrency
caps, row limit, statement timeout, tier permissions, per-user exceptions and caps.
Moving to a larger instance changes nothing about the design.

**Sticky** — needs a migration or breaks existing clients: the `api_keys` and `audit_log`
schemas, the permission-string grammar once keys are issued, and the structured-writes
decision.

## Deferred

- **Audit log retention.** The table grows without bound. Low volume makes this slow, but
  a year of daily use is real. Add a retention job later; it should be a known task
  rather than a surprise.
- **Rejecting oversized requests before execution.** The row limit truncates after the
  fact. Estimating cost up front — via `EXPLAIN` — would reject earlier.
- **Per-schema read permissions.** Requires additional read-only Postgres roles.
- **Complete read attribution.** Requires per-person Postgres accounts and removing
  direct pgAdmin access.
- **Per-tier write roles.** Splitting `api_writer` into `api_writer_open` (research,
  synthetic, macro_data, eia, metadata) and `api_writer_admin` (everything) would make
  write *scope* enforced by Postgres rather than by the gate alone. Deferred because it
  interacts awkwardly with per-user exceptions: an exception granting a quant_dev write
  access to a protected schema would be admitted by the gate and then refused by the
  role. Worth doing if exceptions turn out to be rare, or if the gate grows complex
  enough that a bug in it becomes plausible.
