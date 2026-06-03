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

---

## trigger_step_function.py

Triggers the data step function (ephemeral RDS restore → optional date drifting → optional
anonymisation → rename dance). Fetches DB credentials from Doppler automatically.

```bash
mise run trigger
# or: uv run trigger_step_function.py
```

**Interactive flow**

1. **Operation** — `drift` or `anonymisation`
   - `drift` → `drifting=true`, `anonymisation=false`, Doppler config `int`
   - `anonymisation` → `drifting=false`, `anonymisation=true`, Doppler config `prod`
2. **Snapshot** — pick from own-account snapshots (drift) or shared snapshots (anonymisation)
3. **Target RDS** — pick from instance list (filtered to `test`/`stg` instances)

All parameters can be passed as flags to skip the interactive prompts:

| Option | Default | Description |
|---|---|---|
| `--operation`, `-o` | prompted | Operation: `drift` or `anonymisation` |
| `--snapshot-arn`, `-s` | interactive list | ARN of the RDS snapshot to restore from |
| `--target-rds-instance-id`, `-t` | interactive list | Target RDS instance ID |
| `--watch` / `--no-watch` | `--watch` | Stream execution progress after triggering |
| `--debug` | `false` | Also stream ECS task CloudWatch logs in real time (implies `--watch`) |
| `--dry-run` | `false` | Print the payload without triggering |

**Examples**

```bash
# Fully interactive
mise run trigger

# Non-interactive drift
uv run trigger_step_function.py \
  --operation drift \
  --snapshot-arn arn:aws:rds:eu-west-3:418484240945:snapshot:golden-snapshot-20260305-postgres-18 \
  --target-rds-instance-id db-test2

# Non-interactive anonymisation
uv run trigger_step_function.py \
  --operation anonymisation \
  --snapshot-arn arn:aws:rds:eu-west-3:123456789012:snapshot:shared-snapshot-20260305 \
  --target-rds-instance-id db-test2

# Dry-run to inspect the payload
uv run trigger_step_function.py --operation drift --dry-run
```

If the execution fails, the CLI will prompt to clean up the stale SSM context
(`/opac/int/step_function/context`) automatically.

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

Execs into a running ECS container via SSM `execute-command`. Interactively lists available clusters and tasks if not provided as flags.

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

## Container images

Image build contexts live under `images/`:

- `images/data-step-function/` — drift/anonymisation/rename-dance images and ASL.
- `images/stg-prod-restore/` — staging production-derived restore tooling images used by the Terraform restore stack.

Build and push one image at a time:

```bash
images/build-push.sh stg-prod-restore-db-admin
images/build-push.sh stg-prod-restore-sanitizer
```

The default tag is the current git SHA. Override with an explicit second argument when needed:

```bash
images/build-push.sh stg-prod-restore-db-admin 2026-06-03-1
```
