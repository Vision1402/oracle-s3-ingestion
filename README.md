# Oracle → S3 Raw Layer: Config-Driven Ingestion Framework (practice project)

A hands-on practice project that mirrors a common enterprise setup: **Oracle tables moved
to an AWS S3 raw layer** by a **config-driven Python framework** that runs on a **Linux
edge node**, which engineers reach with **PuTTY**.

> Practice project with synthetic data (a fictional retailer, "Northwind", and its marketing team).
> It contains no real company code, data or names.

## The story

Northwind's marketing team runs campaigns (email, SMS, push) from an in-house tool whose data
lives in **Oracle**. The company is moving to the cloud. **Phase 1 lands every Oracle table in
an S3 raw layer**, untouched, so later phases (a warehouse such as Snowflake, analytics, ML)
can be built on it.

There are dozens of tables, so nobody writes a new script per table. A small framework does the
work, and **onboarding a table means adding one YAML file**.

## Architecture

```
 Windows laptop                 Linux edge node (Docker)                    AWS
┌─────────────┐  SSH  ┌──────────────────────────────────┐        ┌──────────────────────────────┐
│ PuTTY       │──────►│ bin/run_table.sh  (scheduler     │        │ S3 bucket                    │
│ (terminal)  │ :2222 │   calls this; exit code 0/1/2)   │        │  raw/oracle/<area>/<table>/  │
└─────────────┘       │        │                         │        │    load_date=YYYY-MM-DD/     │
                      │        ▼                         │ boto3  │      <batch>-part-00000.parquet
┌─────────────┐       │ python -m dif.run                │───────►│  _control/watermarks/*.json  │
│ Oracle      │◄──────│  config/env/dev.yaml             │        │  _control/audit/...json      │
│ (Docker)    │  SQL  │  config/tables/*.yaml  ◄── one   │        └──────────────┬───────────────┘
│ NORTHWIND   │ read  │                          per     │                       │
│ schema      │ only  │  logs/dif_<run_id>.log   table   │        Glue Data Catalog + Athena
└─────────────┘       └──────────────────────────────────┘        (SQL over S3, for validation)
```

### What happens when one table loads

| Step | What | Why |
|------|------|-----|
| 1. Plan | full: whole table · incremental: rows where `watermark_column` is after the last successful run | Only move what changed |
| 2. Expected numbers | `COUNT(*)` and `SUM(...)` in Oracle, inside a **READ ONLY transaction** | Count and extract see the same snapshot |
| 3. Extract | chunks of rows → Parquet (snappy) → S3 | Large tables never fill memory |
| 4. Validate | rows written = Oracle count, sums match exactly | Prove nothing was lost |
| 5. Commit / roll back | success: advance the watermark, or replace the old snapshot · failure: delete this run's files | The raw layer never holds half a load |
| 6. Audit | JSON record per run in `_control/audit/` | Evidence for every load |

### Type rules (from the Oracle assessment)

| Oracle | Parquet | Why |
|--------|---------|-----|
| `DATE`, `TIMESTAMP` | timestamp(µs) | Oracle DATE contains a time of day |
| `NUMBER(p,0)`, p ≤ 18 | int64 | IDs and counts |
| `NUMBER(p,s)` | decimal(p,s) | Money stays exact |
| `NUMBER` (no precision) | decimal(38,10) | **Fails loudly** if a value has more than 10 decimals; never rounds silently |
| `VARCHAR2`, `CHAR`, `CLOB` | string (UTF-8) | Commas, quotes and newlines are safe in Parquet |

Every file also gets `_batch_id` and `_ingested_at` columns, so each row can be traced to its load.

## Repository layout

```
dif/                    the framework (Python package)
  config.py             load + validate YAML (blocks SQL injection via identifiers)
  oracle_source.py      read-only, chunked Oracle reads (exact Decimals, CLOBs as text)
  typemap.py            Oracle → Parquet type rules
  s3_target.py          Parquet writes, deletes, watermarks, audit records
  pipeline.py           one table: plan → count → extract → validate → commit/rollback → audit
  run.py                command line: --table / --all / --list / --dry-run / --reset-watermark
config/env/dev.yaml     bucket, region, Oracle DSN (no passwords)
config/tables/*.yaml    ONE FILE PER TABLE  ← onboarding a table = adding a file here
bin/run_table.sh        wrapper that a scheduler calls
oracle/                 scripts that build the Oracle source (Northwind + marketing tables)
docker/edge-node/       the Linux server image (SSH, Python, AWS CLI, git, cron)
aws/iam_policy_dif.json least-privilege policy for the framework's AWS user
athena/                 Athena DDL + validation queries
docs/putty_and_linux.md PuTTY, SSH keys, Linux commands, schedulers
tests/                  automated tests (fake Oracle + fake S3, no accounts needed)
```

## Practice backlog

Work through these like real tickets: branch, commit with the key, PR, merge.

| Ticket | Title | Done when |
|--------|-------|-----------|
| **MKT-1** | AWS setup: budget alert, S3 bucket, IAM user + least-privilege policy | `aws s3 ls s3://<bucket>` works with the framework's keys |
| **MKT-2** | Add marketing tables to Oracle | `CAMPAIGNS` and `CAMPAIGN_EVENTS` exist; golden numbers recorded |
| **MKT-3** | Build the edge node and connect with PuTTY | You're logged in as `etluser` and see the banner |
| **MKT-4** | Deploy the framework: clone, configure, dry-run | `--list` shows 6 tables; a `--dry-run` shows row counts |
| **MKT-5** | First load of all tables | 6 × SUCCESS; S3 counts = Oracle golden numbers |
| **MKT-6** | Incremental load + idempotent re-run | New activity loads once; a second run loads 0 rows |
| **MKT-7** | Glue crawler + Athena validation | Athena counts and sums match Oracle |
| **MKT-8** | Onboard EMPLOYEES with config only (contains PII!) | New YAML + PR + a written decision on salary/national_id |
| **MKT-9** | Break it and fix it: debug 3 failures from logs only | Root cause written for each |
| **MKT-10** | Schedule it with cron; check exit codes | The job runs by itself; the log shows SUCCESS |

## Setup, step by step

### MKT-1: AWS (in the AWS console, on your laptop)
1. **Billing → Budgets → Create budget**: a monthly cost budget of **$5** with an email alert. Do this first.
2. **S3 → Create bucket**: name `vision-mkt-raw-dev` (must be globally unique), region **us-east-1**,
   keep **Block all public access ON**, default encryption **SSE-S3**.
3. **IAM → Users → Create user** `dif-etl-dev`, with **no console access**.
   Add an inline policy: paste `aws/iam_policy_dif.json`, replacing `YOUR-BUCKET-NAME` (twice).
4. In that user, open **Security credentials → Create access key → Command Line Interface**.
   Save the key ID and secret in a password manager, **never in Git**.

> At a bank, servers usually get an **IAM role** instead of access keys, so no secret is ever
> typed. Keys are fine for a personal practice account.

### MKT-2: Marketing tables in Oracle (PowerShell, in this project folder)
```powershell
docker start oracle-legacy
docker cp oracle/03_marketing_tables.sql oracle-legacy:/tmp/03_marketing_tables.sql
docker exec -it oracle-legacy sqlplus northwind/Northwind123@//localhost:1521/FREEPDB1 '@/tmp/03_marketing_tables.sql'
```
Write down the printed counts. They're your golden numbers for CAMPAIGNS and CAMPAIGN_EVENTS.

### MKT-3: Edge node + PuTTY (PowerShell, in this project folder)
```powershell
docker network create dif-net
docker network connect dif-net oracle-legacy
docker build -t dif-edge-node -f docker/edge-node/Dockerfile .
docker run -d --name edge-node --hostname edge-node --network dif-net -p 2222:22 dif-edge-node
```
Then follow **docs/putty_and_linux.md** to connect: `localhost`, port `2222`, user `etluser`.

### MKT-4: Deploy and configure (inside PuTTY)
```bash
git clone https://github.com/<your-github-username>/oracle-s3-ingestion.git
cd oracle-s3-ingestion

# Oracle credentials for the framework (readable only by you)
cat > ~/.dif_env <<'EOF'
export ORACLE_USER=northwind
export ORACLE_PASSWORD=Northwind123
EOF
chmod 600 ~/.dif_env

# AWS credentials (key ID, secret, region us-east-1, output json)
aws configure
aws s3 ls s3://<your-bucket>          # empty output = access works
```
Set your bucket name in `config/env/dev.yaml` **on your laptop**, commit it, open a PR, merge, and
then run `git pull` on the edge node. Config changes go through Git, never by editing on the server.
```bash
python3 -m dif.run --env dev --list
bash bin/run_table.sh dev northwind_products --dry-run
```

### MKT-5 onwards
```bash
bash bin/run_table.sh dev --all                 # load everything
echo $?                                         # 0 = all good
aws s3 ls s3://<your-bucket>/raw/ --recursive --human-readable
```
For MKT-6, run `EXEC simulate_new_activity(500)` in DBeaver, wait 3 minutes (the framework ignores
the most recent 2 minutes, `lag_minutes`), then run `--all` again, and then once more.

## Running the tests (optional, on any machine with Python)
```bash
pip install -r requirements-dev.txt
python -m pytest -q
```
The tests use a fake Oracle and a fake S3 (moto). They cover full loads, incremental loads,
idempotent re-runs, rollback on a failed validation, decimal precision and config validation.

## Costs

S3 storage for this data is a few cents. Athena charges about $5 per TB scanned (this project
scans megabytes). A Glue crawler run costs a few cents. Keep the $5 budget alert on, and delete the
bucket when you're done practicing.
