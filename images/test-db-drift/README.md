# Test DB Drift

ECS task images for the test-db-drift workflow: ephemeral RDS restore with date
drifting, rename dance, and cleanup.

The Step Function and all infrastructure (ECS cluster, Lambda, IAM, DynamoDB,
VPC networking) is managed via Terraform in
[opac-terraform](https://github.com/opk/opac-terraform) under
`envs/non-prod/test-db-drift/`.

## Tasks

| Task | Image | Purpose |
|---|---|---|
| **drifting** | `test-db-drift-drifting` | Create ephemeral RDS from snapshot, wait for availability, apply date-drifting UPDATEs to 19 tables |
| **rename-dance** | `test-db-drift-rename-dance` | Rename target → `-old`, rename ephemeral → target, delete old target, verify connectivity |
| **cleanup** | `test-db-drift-cleanup` | Catch-all error path: delete ephemeral RDS if one exists (always exits 1 to mark SFN as FAILED) |

## Architecture

- **No SSM Parameter Store** — state is passed via individual env vars through SFN container overrides
- **No anonymisation** — removed; drift-only workflow
- **Deterministic ephemeral naming** — ephemeral ID is `ephemeral-transform-<targetId>`, making it idempotent
- **Locking** — DynamoDB table (`opk-opac-test-db-drift-state`) with `attribute_not_exists(pk)` condition
- **CLI** — `test_db.py` at repo root triggers the SFN via boto3

## Push a Docker image to ECR

```bash
# Authenticate
aws ecr get-login-password --region eu-west-3 \
  | docker login --username AWS --password-stdin \
      884080474326.dkr.ecr.eu-west-3.amazonaws.com

# Build & push
images/build-push.sh test-db-drift-drifting
images/build-push.sh test-db-drift-rename-dance
images/build-push.sh test-db-drift-cleanup
```

Each image is tagged with the current git SHA (e.g. `test-db-drift-drifting-abc1234`).
Override with a second argument when needed:

```bash
images/build-push.sh test-db-drift-drifting 2026-06-05-1
```

## Drifting Step Local Dev

To run the drifting step locally, export the env vars that the SFN would normally
inject via container overrides:

```bash
export TARGET_RDS_INSTANCE_ID="opk-opac-test2"
export SNAPSHOT_ARN="arn:aws:rds:eu-west-3:418484240945:snapshot:golden-snapshot-20260305-postgres-18"
export SNAPSHOT_DB_NAME="opac"
export SNAPSHOT_DB_USERNAME="postgres"
export SNAPSHOT_DB_PASSWORD="<password>"
export AWS_REGION="eu-west-3"
export DRIFT_MAX_WORKERS="8"
export DRIFT_BATCH_TIMEOUT_SECONDS="3600"

uv run images/test-db-drift/src/step-drifting.py
```

## Step Function Input

```json
{
  "comment": "Triggered via CLI",
  "snapshotArn": "arn:...",
  "snapshotDbName": "opac",
  "snapshotDbUsername": "postgres",
  "snapshotDbPassword": "●●●●●●●●",
  "targetRdsInstanceId": "opk-opac-test2"
}
```

## Local Dev

### Generate a token for RDS IAM auth

```bash
export AUTH_TOKEN=$(aws rds generate-db-auth-token \
  --hostname opk-opac-test2.c4k4uoc9kxxx.eu-west-3.rds.amazonaws.com \
  --username postgres --port 5432)
```

NB: the ephemeral uses the same credentials as the source snapshot, so an IAM
token is not needed when connecting via bastion tunnel.