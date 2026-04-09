# aws-utils

Collection of developer utilities for interacting with the OPAC AWS infrastructure.

## Prerequisites

- [uv](https://docs.astral.sh/uv/getting-started/installation/) — `curl -LsSf https://astral.sh/uv/install.sh | sh`
- [AWS CLI v2](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) — `brew install awscli`
- [session-manager-plugin](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html) — `brew install --cask session-manager-plugin`
- [Granted](https://docs.commonfate.io/granted/getting-started) — `brew tap common-fate/granted && brew install granted`

Authenticate before running any script:

```bash
assume onepark-nonprod   # or onepark-prod
```

---

## bastion_connect.py

Opens an SSM port-forwarding tunnel through the shared bastion EC2 to a private RDS instance.

Automatically manages the `/etc/hosts` entry required for PostgreSQL SSL hostname verification,
and removes it on exit (Ctrl+C or normal termination).

**Usage**

```bash
uv run bastion_connect.py [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `--rds`, `-r` | — | RDS instance ID (shows interactive list if omitted) |
| `--bastion`, `-b` | `opk-opac-shared-bastion` | Bastion EC2 instance ID (`i-xxx`) or Name tag |
| `--local-port`, `-p` | `5432` | Local port to bind |
| `--rds-port` | `5432` | Remote RDS port |

**Examples**

```bash
# Interactive RDS selection (most common)
uv run bastion_connect.py

# Direct — skip the listing
uv run bastion_connect.py --rds db-test2

# Custom local port (if 5432 is already in use)
uv run bastion_connect.py --rds db-test2 --local-port 5433
```

Once the tunnel is open, connect with psql using the full RDS hostname (not `localhost`):

```bash
psql -h <rds-hostname> -p 5432 -U <user> -d <db>
```
