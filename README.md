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

Opens an SSM port-forwarding tunnel through the shared bastion EC2 to a private resource. Two subcommands are available.

```bash
mise run bastion rds
mise run bastion ecs
```

---

### rds

Lists all RDS instances and lets you pick one interactively. Automatically adds the RDS hostname to `/etc/hosts` (required for PostgreSQL SSL hostname verification) and removes it on exit.

```bash
mise run bastion rds
```

| Option | Default | Description |
|---|---|---|
| `--local-port`, `-p` | `5432` | Local port to bind |

Once the tunnel is open, connect using the RDS hostname (not `localhost`):

```bash
psql -h <rds-hostname> -p 5432 -U <user> -d <db>
```

---

### ecs

Lists all ECS clusters and lets you pick a stage interactively. Tunnels through the bastion to the internal ALB which routes to the correct ECS service. Does **not** modify `/etc/hosts` — your browser is unaffected.

```bash
mise run bastion ecs
```

| Option | Default | Description |
|---|---|---|
| `--local-port`, `-p` | `8443` | Local port to bind |

Once the tunnel is open, use any HTTP client (Postman, curl, httpie, etc.) to make requests. SSL verification must be disabled since the cert is issued for the ALB hostname, not `localhost`. Example with httpie:

```bash
https --verify=no --json POST https://localhost:8443/<path> \
  "Host:<api-hostname>" \
  query='...' \
  variables:='{...}'
```

**Example — Login on int:**

```bash
https --verify=no POST https://localhost:8443/api/cpm/graphql \
  "Host:api.opac.int.onepark.dev" \
  query='mutation Login($input: LoginInput!) { login(input: $input) { jwt } }' \
  variables:='{"input": {"email": "<email>", "password": "<password>", "slugId": "<slugId>"}}'
```
