# opac-devops

## Run the AWS Step Functions
To run your AWS Step Function from the CLI, you use the `start-execution` command. Because the input 
is a complex JSON object, the most reliable way to handle it is by saving it to a temporary file or 
properly escaping the JSON string.

### Option 1: Using a JSON File 
This is the cleanest method to avoid shell escaping errors with special characters or long strings.

Save your input to a file named input.json:

```json
{
    "comment": "inputs for state machine flow. It will be used as 'state' between step functions",
    "snapshotArn": "arn:aws:rds:eu-west-3:418484240945:snapshot:golden-snapshot-20260305-postgres-18",
    "snapshotDbName": "opac",
    "snapshotDbUsername": "<db_snapshot_username>",
    "snapshotDbPassword": "<db_snapshot_password>",
    "snapshotDbPort": 5432,
    "targetRdsInstanceId": "target-instance-test",
    "anonymisation": false,
    "drifting": true
}
```

NB: in this example, we ask for no anonymisation but we ask for date drifting 

Run the command referencing that file:

```shell
aws stepfunctions start-execution \
--state-machine-arn "arn:aws:states:eu-west-3:418484240945:stateMachine:drift-anonymisation-state-machine" \
--input file://input.json
```

### Option 2: Inline String (Bash/Zsh)
If one prefers not to create a file, you can pass the JSON as a single-quoted string. 
Note that you must ensure any internal quotes are handled correctly.

```shell
aws stepfunctions start-execution \
--state-machine-arn "arn:aws:states:eu-west-3:418484240945:stateMachine:drift-anonymisation-state-machine" \
--input '{
"comment": "inputs for state machine flow",
"snapshotArn": "arn:aws:rds:eu-west-3:418484240945:snapshot:golden-snapshot-20260305-postgres-18",
"snapshotDbName": "opac",
"snapshotDbUsername": "<db_snapshot_username>",
"snapshotDbPassword": "<db_snapshot_password>",
"snapshotDbPort": 5432,
"targetRdsInstanceId": "target-instance-test",
"anonymisation": false,
"drifting": false
}'
```

NB: in this example, we ask for both, anonymisation and date drifting

## Local Dev

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
