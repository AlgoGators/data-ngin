# Deploying the database API gateway

The service runs on `data-ngin.algogators.com`. Steps are ordered, and each one
depends on the one before it: the certificate cannot be issued before DNS
resolves, and DNS cannot be pointed anywhere stable before the address is.

## 0. Before you touch the server

**The working tree on the server has uncommitted changes that are not in the
repository. A `git checkout`, `git stash` or `git reset --hard` destroys them.**

Known local-only state, as of this writing:

- `docker-compose.yml` has `AIRFLOW__WEBSERVER__EXPOSE_CONFIG: "non-sensitive-only"`
  on the Airflow services. The repository still says `"true"`, which exposes the
  full Airflow configuration -- including connection strings -- to anyone who
  reaches the webserver.
- `src/config/config.yaml` and `new_config.yaml` have `start_date: ""` set. That
  empty value is what makes the pipelines run incrementally instead of
  re-fetching history on every run.

So: run `git status` and `git diff` on the server first, and keep the output.
Pull with `git pull --rebase` (or fetch and merge), never with a reset. This
change adds an `api:` service to `docker-compose.yml`, which is one of the files
carrying local edits -- expect to resolve that hunk by hand rather than taking
either side wholesale.

## 1. Elastic IP

An Elastic IP was allocated on 2026-08-06 and released the same day. **It is
gone; allocate a fresh one at deploy time.** Do not assume the address recorded
anywhere from that day still belongs to us.

It is not optional. The instance's auto-assigned public address changes whenever
it is stopped and started -- which is exactly what happens when the instance is
resized, and this box is small enough that resizing is a live possibility. The
DNS record would then point at an address we no longer hold, and the failure is
silent until someone reports a timeout.

Associating the Elastic IP changes the server's public address, which breaks
every member's saved pgAdmin connection. Do it in the same window as a password
rotation so the team absorbs one disruption instead of two.

## 2. DNS

Add an A record: `data-ngin.algogators.com` -> the Elastic IP.

`algogators.com` is served from GitHub Pages. This is an independent A record on
a subdomain; no traffic passes through that site, and nothing about the Pages
setup needs to change.

Confirm it resolves before continuing -- Let's Encrypt validates over the name,
so a stale record means a failed certificate rather than a clear error:

    dig +short data-ngin.algogators.com

## 3. Database roles and tables

Both migrations, in order. 003 creates the roles and the three service logins;
004 creates `auth.api_keys` and `auth.audit_log` and grants `api_service_ro` the
direct access it needs to authenticate callers.

    psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f migrations/003_api_gateway_roles.sql
    psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f migrations/004_api_gateway_tables.sql

Both are idempotent and safe to re-run.

### Passwords: three logins, three passwords

003 creates `api_service_ro`, `api_service_rw` and `api_service_all`, each with
the placeholder password `CHANGE_ME_BEFORE_DEPLOY`. There are three rather than
one because Postgres authorises `SET ROLE` against `session_user`: a single
login that was a member of all three roles could be escalated by any caller
prefixing `SET ROLE db_readwrite_all;` to their SQL, and this service
deliberately never inspects the SQL it is given. Giving each role its own login
means Postgres refuses that outright.

Generate three distinct passwords and set them:

    openssl rand -base64 32   # once per login

    ALTER ROLE api_service_ro  WITH PASSWORD '<generated-ro>';
    ALTER ROLE api_service_rw  WITH PASSWORD '<generated-rw>';
    ALTER ROLE api_service_all WITH PASSWORD '<generated-all>';

Then put all three in `.env` on the server, alongside the host and database
name (`.env` is gitignored; see `.env.template`):

    API_DB_HOST=...
    API_DB_PORT=5432
    API_DB_NAME=...
    API_DB_PASSWORD_RO=...
    API_DB_PASSWORD_RW=...
    API_DB_PASSWORD_ALL=...

Do not reuse one password across the three logins. The point of the split is
that a leaked read-only credential reaches no further than read-only access.

Confirm none of them is still the placeholder before starting the service:

    SELECT rolname FROM pg_authid
     WHERE rolname LIKE 'api_service%'
       AND rolpassword = 'md5' || md5('CHANGE_ME_BEFORE_DEPLOY' || rolname);

Expect zero rows. (On a cluster using SCRAM this returns zero rows regardless,
so treat it as a smoke test, not proof.)

## 4. Caddy

    sudo apt install -y caddy
    sudo cp Caddyfile /etc/caddy/Caddyfile
    sudo systemctl reload caddy

Caddy obtains the certificate on first start. Open 443 in the security group;
80 must also be reachable for the ACME HTTP challenge. Leave 8000 closed -- the
container binds to `127.0.0.1:8000`, and Caddy is the only public listener.

Watch the first issuance rather than assuming it:

    sudo journalctl -u caddy -f

## 5. The service

    docker compose up -d api
    curl https://data-ngin.algogators.com/healthz

Expect `{"status":"ok"}`. `docker compose up -d api` starts only the API; it
does not restart Airflow.

If the container exits immediately, `docker compose logs api`. A missing
`API_DB_PASSWORD_*` fails at compose parse time with a message naming the
variable, so a container that starts and then dies is a database connection
problem, not a missing variable.

## 6. First key

The admin CLI connects with the **superuser** credentials (`DB_HOST`, `DB_USER`,
`DB_PASSWORD`, `DB_NAME`), not the API's logins -- writing to `auth.api_keys` is
deliberately out of reach of everything the service itself can do.

    python -m scripts.manage_api_keys create \
        --email you@example.com --name "Your Name" --role db_readwrite_all

The plaintext key is printed once and is not recoverable. Only its SHA-256 hash
is stored. If it is lost, re-run the same `create` command: it upserts on email,
overwriting the hash, so the previous key stops working immediately. Revoking is
`manage_api_keys revoke --email ...`, which deactivates the row but keeps it, so
"who had access in March?" stays answerable.

## Verifying it works

    curl -s https://data-ngin.algogators.com/v1/query \
      -H "Authorization: Bearer $KEY" \
      -H "Content-Type: application/json" \
      -d '{"sql":"SELECT count(*) FROM equities_data.ohlcv_1d"}'

Then confirm it was recorded -- an unaudited success is a deployment that is
half working:

    SELECT actor_name, outcome, statement, occurred_at
      FROM auth.audit_log ORDER BY id DESC LIMIT 5;

Worth checking once, with a read-only key, that enforcement is live:

    curl -s https://data-ngin.algogators.com/v1/query \
      -H "Authorization: Bearer $READONLY_KEY" \
      -H "Content-Type: application/json" \
      -d '{"sql":"SET ROLE db_readwrite_all; SELECT 1"}'

Expect a 403 or a Postgres error, and a `denied` row in the audit log. A success
here means the three logins were not applied and every key is an admin key.

## Rolling back

    docker compose stop api

That is the whole rollback for the service. The roles and tables are additive
and harm nothing if left in place -- leaving them keeps the audit history.

To remove them entirely:

    DROP TABLE auth.audit_log, auth.api_keys;
    DROP ROLE api_service_all, api_service_rw, api_service_ro;
    DROP ROLE db_readwrite_all, db_readwrite, db_readonly;

`DROP ROLE` fails if a role owns objects or holds grants. Clear them first:

    REASSIGN OWNED BY db_readwrite TO postgres;
    DROP OWNED BY db_readwrite;

Dropping the audit log discards the record of who ran what. Export it first if
there is any chance it will be wanted.
