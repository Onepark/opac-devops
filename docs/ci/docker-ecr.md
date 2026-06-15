# CI: Docker → ECR

The [`docker-ecr`](/.github/workflows/docker-ecr.yml) workflow builds the five
container images under `images/` and pushes them to Amazon ECR.

## GitHub Actions variable

| Variable | Scope | Description |
|---|---|---|
| `AWS_DEPLOY_ROLE_ARN` | Repository | IAM role ARN for OIDC auth (pre-provisioned in IaC) |

Set it under **Settings → Secrets and variables → Actions → Variables**.

## IAM role permissions

The OIDC role needs at minimum:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ecr:GetAuthorizationToken",
        "ecr:BatchCheckLayerAvailability",
        "ecr:InitiateLayerUpload",
        "ecr:UploadLayerPart",
        "ecr:CompleteLayerUpload",
        "ecr:PutImage",
        "ecr:BatchGetImage"
      ],
      "Resource": "arn:aws:ecr:eu-west-3:884080474326:repository/opac-devops"
    }
  ]
}
```

> `ecr:GetAuthorizationToken` must be allowed on `*` (AWS requirement).

## OIDC trust policy

The trust policy must allow `token.actions.githubusercontent.com` with a `sub`
claim scoped to this repository. Example:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::884080474326:oidc-provider/token.actions.githubusercontent.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
        },
        "StringLike": {
          "token.actions.githubusercontent.com:sub": "repo:<org>/<repo>:*"
        }
      }
    }
  ]
}
```

Replace `<org>/<repo>` with the actual GitHub org/repo path.

## When images are built

| Event | Build | Push to ECR |
|---|---|---|
| Pull request | Yes (path-filtered) | No |
| Push to non-main branch | Yes (path-filtered) | No |
| Push to `main` | Yes (path-filtered) | Yes |
| Tag push (`refs/tags/*`) | Yes (all images) | Yes |
| `workflow_dispatch` | Yes (all images) | Yes |

Path filtering: only images whose context directory changed are built. A change
to `.github/workflows/**`, `mise.toml`, or `images/build-push.sh` forces all
images to rebuild.

## Image tags

Each image is tagged `<image-name>-<short-sha>`, e.g.

```
884080474326.dkr.ecr.eu-west-3.amazonaws.com/opac-devops:test-db-drift-drifting-abcdef0
```

No `latest` tag is applied. Consumers (ECS task definitions, Terraform) must
reference explicit tags.

## Manual dispatch

1. Go to **Actions → docker-ecr**
2. Click **Run workflow**
3. Select the branch (defaults to `main`)
4. All five images are built and pushed with the current HEAD short SHA

## Verify a push

```bash
aws ecr describe-images \
  --repository-name opac-devops \
  --region eu-west-3 \
  --image-ids imageTag=test-db-drift-drifting-abcdef0
```

## Branch protection

The workflow exposes check names like `Build test-db-drift-drifting`. To require
them before merge, add them under **Settings → Branches → Branch protection
rules → Require status checks to pass before merging**.
