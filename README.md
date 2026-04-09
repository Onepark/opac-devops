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

All parameters can be passed as flags or filled in interactively:

| Option | Default | Description |
|---|---|---|
| `--mode`, `-m` | prompted | Environment: `int` or `prod` |
| `--snapshot-arn`, `-s` | prompted | ARN of the RDS snapshot to restore from |
| `--target-rds-instance-id`, `-t` | interactive list | Target RDS instance ID |
| `--anonymisation` / `--no-anonymisation` | prompted | Enable data anonymisation |
| `--drifting` / `--no-drifting` | prompted | Enable date drifting |
| `--watch` / `--no-watch` | `--watch` | Stream execution progress after triggering |
| `--debug` | `false` | Also stream ECS task CloudWatch logs in real time (implies `--watch`) |
| `--dry-run` | `false` | Print the payload without triggering |

**Examples**

```bash
# Fully interactive
mise run trigger

# Non-interactive
uv run trigger_step_function.py \
  --mode int \
  --snapshot-arn arn:aws:rds:eu-west-3:418484240945:snapshot:golden-snapshot-20260305-postgres-18 \
  --target-rds-instance-id db-test2 \
  --drifting --no-anonymisation

# Dry-run to inspect the payload
uv run trigger_step_function.py --mode int --dry-run
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
