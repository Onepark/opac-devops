# OPAC DevOps

Collection of developer utilities for interacting with the OPAC AWS infrastructure.

## Setup

### 1 — Install mise

[mise](https://mise.jdx.dev) manages tool versions and provides task shortcuts.

```bash
brew install mise
mise install        # installs uv (and any other tools declared in mise.toml)
```

Add the following to your shell profile if not already done:

```bash
# ~/.zshrc
eval "$(mise activate zsh)"
```

### 2 — Install remaining dependencies

```bash
mise install        # installs uv, awscli, doppler, granted
brew install --cask session-manager-plugin   # not available in mise
```

### 3 — Authenticate

```bash
assume onepark-nonprod   # or onepark-prod
```

### 4 — Install pre-commit hooks

```bash
brew install pre-commit
pre-commit install
```

Hooks run `ruff check` and `ruff format` on each subproject under `images/`
using each subproject's own `pyproject.toml` config. Run on demand with
`pre-commit run --all-files`.

---

## test_db.py

Triggers the test-db-drift Step Function (ephemeral RDS restore from own-account
manual snapshot → date drifting → rename dance). Fetches DB credentials from
Doppler (`opac-data-step-function` / `int` config).

```bash
uv run test_db.py
```

**Interactive flow**

1. **Snapshot** — pick from own-account PostgreSQL manual snapshots (filtered to
   those with a date in the name for drift computation)
2. **Target RDS** — pick from instance list (filtered to `test` instances)

All parameters can be passed as flags to skip the interactive prompts:

| Option | Default | Description |
|---|---|---|
| `--snapshot-arn`, `-s` | interactive list | ARN of the RDS snapshot to restore from |
| `--target-rds-instance-id`, `-t` | interactive list | Target RDS instance ID |
| `--watch` / `--no-watch` | `--watch` | Stream execution progress after triggering |
| `--debug` | `false` | Also stream ECS task CloudWatch logs in real time (implies `--watch`) |
| `--dry-run` | `false` | Print the payload without triggering |

**Examples**

```bash
# Fully interactive
uv run test_db.py

# Non-interactive
uv run test_db.py \
  --snapshot-arn arn:aws:rds:eu-west-3:418484240945:snapshot:golden-snapshot-20260305-postgres-18 \
  --target-rds-instance-id opk-opac-test2

# Dry-run to inspect the payload
uv run test_db.py --dry-run
```

---

## bastion_connect.py

Opens an SSM port-forwarding tunnel through the shared bastion EC2 to a private RDS instance.

Automatically manages the `/etc/hosts` entry required for PostgreSQL SSL hostname verification,
and removes it on exit (Ctrl+C or normal termination).

```bash
mise run bastion
# or: uv run bastion_connect.py
```

| Option | Default | Description |
|---|---|---|
| `--rds`, `-r` | interactive list | RDS instance ID |
| `--bastion`, `-b` | `opk-opac-shared-bastion` | Bastion EC2 instance ID (`i-xxx`) or Name tag |
| `--local-port`, `-p` | `5432` | Local port to bind |
| `--rds-port` | `5432` | Remote RDS port |

**Examples**

```bash
# Interactive RDS selection (most common)
mise run bastion

# Direct — skip the listing
uv run bastion_connect.py --rds db-test2

# Custom local port (if 5432 is already in use)
uv run bastion_connect.py --rds db-test2 --local-port 5433
```

Once the tunnel is open, connect with psql using the full RDS hostname (not `localhost`):

```bash
psql -h <rds-hostname> -p 5432 -U <user> -d <db>
```

---

## ecs_exec.py

Execs into a running ECS container via SSM `execute-command`. Interactively lists
available clusters and tasks if not provided as flags.

```bash
mise run ecs-exec
# or: uv run ecs_exec.py
```

| Option | Default | Description |
|---|---|---|
| `--cluster`, `-c` | interactive list | ECS cluster ARN or name |
| `--task`, `-t` | interactive list | ECS task ID |
| `--container` | `api` | Container name |
| `--command` | `/bin/sh` | Command to run inside the container |

**Examples**

```bash
# Fully interactive
mise run ecs-exec

# Skip cluster selection
uv run ecs_exec.py --cluster opk-opac-test2-ecs-cluster

# Skip both cluster and task selection
uv run ecs_exec.py --cluster opk-opac-test2-ecs-cluster --task <task-id>
```

**Running a worker from inside the container**

Once connected, start an IEx session attached to the running node:

```bash
/app/bin/opac remote
```

Then insert a job from IEx:

```elixir
# Trigger an accounting report for the current month
Oban.insert(OpacCore.Payments.Workers.AccountingReport.new(%{"period" => "monthly"}))

# Force a specific reference date (useful to report on past months with real data)
Oban.insert(OpacCore.Payments.Workers.AccountingReport.new(%{"period" => "monthly", "today" => "2026-06-01"}))
```

---

## stg_db.py

Switches the `db.stg.onepark.dev` Route53 CNAME to the latest ready production-derived
restore slot (the staging DB cutover). Reads slot state from the
`opk-opac-stg-prod-restore-state` DynamoDB table, picks the slot with `readyForQa=true`
and the latest `sourceSnapshotWeek` (ISO year/week, tie-broken on snapshot create
time), moves the cutover, promotes the new active slot, demotes the previous one,
and submits a fire-and-forget `DeleteDBInstance` for the previous active RDS.

Audit-only — writes a `PROMOTION#<iso-week>` record to the same DynamoDB table.

Requires the non-prod developer role to have `AmazonDynamoDBFullAccess` and the
inline Route53 cutover policy attached (added in
`envs/non-prod/shared/main.tf`).

```bash
mise run stg-db switch                 # or: uv run stg_db.py switch
mise run stg-db -- switch --dry-run    # preview only, no AWS mutation
```

| Option | Default | Description |
|---|---|---|
| `--dry-run` | `false` | Print the plan and exit without mutating anything |
| `--requester` | `$USER` | Audit metadata: who is promoting |
| `--reason` | `""` | Audit metadata: why are you promoting? |

**Examples**

```bash
# Preview which slot would be promoted
uv run stg_db.py switch --dry-run

# Promote with a reason for the audit record
uv run stg_db.py switch --reason "QA requested latest snapshot"

# Audit trail lookup
aws dynamodb get-item \
  --table-name opk-opac-stg-prod-restore-state \
  --key '{"pk": {"S": "PROMOTION#2026w23"}}'
```

If no slot is `readyForQa`, the script exits non-zero with
`no slot is ready; trigger or wait for a reconcile`. The cutover is a no-op
(exit 0) if the current Route53 target already points at the chosen slot.

---

## Container images

Image build contexts live under `images/`:

- `images/test-db-drift/` — date-drifting/rename-dance/cleanup images and ASL (deployed via Terraform).
- `images/stg-prod-restore/` — staging production-derived restore tooling images used by the Terraform restore stack.

Build and push one image at a time:

```bash
images/build-push.sh test-db-drift-drifting
images/build-push.sh test-db-drift-rename-dance
images/build-push.sh test-db-drift-cleanup
images/build-push.sh stg-prod-restore-db-admin
images/build-push.sh stg-prod-restore-sanitizer
```

The default tag is the current git SHA. Override with an explicit second argument when needed:

```bash
images/build-push.sh stg-prod-restore-db-admin 2026-06-03-1
```

Images are also built and pushed automatically by the
[`docker-ecr`](/.github/workflows/docker-ecr.yml) GitHub Actions workflow on
push to `main`, tag pushes, and manual dispatch. See
[`docs/ci/docker-ecr.md`](/docs/ci/docker-ecr.md) for setup and operator
notes.
