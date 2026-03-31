# opac-devops

## Lcal Dev

### Push a docker image to ECR

First, Authenticate to the shared account's ECR :  

```
aws ecr get-login-password --region eu-west-3 \
  | docker login \
      --username AWS \
      --password-stdin \
      <account_id>.dkr.ecr.eu-west-3.amazonaws.com
```

Then tag the image you want to push (for instance `step-drifting`)  

# 2. Tag your image
```
docker tag step-drifting:latest \
  <account_id>.dkr.ecr.eu-west-3.amazonaws.com/my-repo:tag
```

And then push to ECR:
```
docker push 884080474326.dkr.ecr.eu-west-3.amazonaws.com/my-repo:tag
```

### modify hosts file
Because we use a ssm session to tunnel to PostgresSQL, we need to change the `hosts` file adding the line (for instance):

```
127.0.0.1  db-test2.c4k4uoc9kxxx.eu-west-3.rds.amazonaws.com
```

With this, psql can point to localhost and go through the ssm sesion to the EC2 shared bastion machine.

### Postgres SQL SSL mode
Download pem file for full ssl verification
```
curl -o ~/rds-certs/global-bundle.pem https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
```

### sso login 
```aws sso login --profile onepark-nonprod```

### start ssm session to PostgreSQL
```aws ssm start-session       --target "<instance id of EC2 shared bastion>"       --document-name "AWS-StartPortForwardingSessionToRemoteHost"       --parameters "{\"host\":[\"db-test2.c4k4uoc9kxxx.eu-west-3.rds.amazonaws.com\"],\"portNumber\":[\"5432\"],\"localPortNumber\":[\"5432\"]}"       --region "eu-west-3"```

## misc
### create a token for an RDS instance
```export AUTH_TOKEN=$(aws rds generate-db-auth-token --hostname db-test2.c4k4uoc9kxxx.eu-west-3.rds.amazonaws.com --username <master-username> --port 5432)```

NB: because the instance created is based on integration snapshot, the password used to connect is the same as the
integration database (cf Parameter Store in AWS console). So the token is not useful.

### Roles & Policies (for later terraformation)

#### Task Role Policy

opk-opac-stepfunction-data : 
```
{
	"Statement": [
		{
			"Action": [
				"rds:*"
			],
			"Effect": "Allow",
			"Resource": "*",
			"Sid": "RDS"
		},
		{
			"Effect": "Allow",
			"Action": [
				"ssm:GetParameter",
				"ssm:PutParameter",
				"ssm:DeleteParameter"
			],
			"Resource": "arn:aws:ssm:eu-west-3:418484240945:parameter/opac/int/step_function/*"
		}
	],
	"Version": "2012-10-17"
}
```

#### Task Role Execution 

`opk-opak-step-function-data-exec` :
```
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Action": [
				"logs:PutLogEvents",
				"logs:CreateLogStream"
			],
			"Effect": "Allow",
			"Resource": "*",
			"Sid": "Logs"
		},
		{
			"Action": [
				"ecr:GetDownloadUrlForLayer",
				"ecr:GetAuthorizationToken",
				"ecr:BatchGetImage",
				"ecr:BatchCheckLayerAvailability"
			],
			"Effect": "Allow",
			"Resource": "*",
			"Sid": "ECR"
		},
		{
			"Action": "ssm:GetParameters",
			"Effect": "Allow",
			"Resource": "arn:aws:ssm:eu-west-3:418484240945:parameter/opac/*",
			"Sid": "GetSSMParams"
		}
	]
}
```

#### Step Function policy

Add RunTask rights for ECS task definitions and iam:PassRole for Task Role and Task Role Execution : 

```
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Effect": "Allow",
			"Action": [
				"ecs:RunTask"
			],
			"Resource": [
				"arn:aws:ecs:eu-west-3:418484240945:task-definition/ecs-nonprod-anonymisation:*",
				"arn:aws:ecs:eu-west-3:418484240945:task-definition/ecs-nonprod-drifting:*",
				"arn:aws:ecs:eu-west-3:418484240945:task-definition/ecs-nonprod-rename-dance:*"
			]
		},
		{
			"Effect": "Allow",
			"Action": [
				"iam:PassRole"
			],
			"Resource": [
				"arn:aws:iam::418484240945:role/opk-opac-stepfunction-data",
				"arn:aws:iam::418484240945:role/opk-opac-stepfunction-data-exec"
			],
			"Condition": {
				"StringLike": {
					"iam:PassedToService": "ecs-tasks.amazonaws.com"
				}
			}
		},
		{
			"Effect": "Allow",
			"Action": [
				"events:PutTargets",
				"events:PutRule",
				"events:DescribeRule"
			],
			"Resource": [
				"arn:aws:events:eu-west-3:418484240945:rule/StepFunctionsGetEventsForECSTaskRule"
			]
		}
	]
}
```
