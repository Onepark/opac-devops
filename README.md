# opac-devops

## local dev

### modify hosts file
Because we use a ssm session to tunnel to PostgresSQL, we need to change the `hosts` file adding the line (for instance):

```
127.0.0.1  db-test2.c4k4uoc9kxxx.eu-west-3.rds.amazonaws.com
```

With this, psql can point to localhost and go through the ssm sesion to the EC2 shared bastion machine.

### Postgres SQL SSL mode
Download pem file for full ssl verification
``` ```

### sso login 
```aws sso login --profile onepark-nonprod```

### start ssm session to PostgreSQL
```aws ssm start-session       --target "<instance id of EC2 shared bastion>"       --document-name "AWS-StartPortForwardingSessionToRemoteHost"       --parameters "{\"host\":[\"db-test2.c4k4uoc9kxxx.eu-west-3.rds.amazonaws.com\"],\"portNumber\":[\"5432\"],\"localPortNumber\":[\"5432\"]}"       --region "eu-west-3"```

## misc
### create a token for an RDS instance
```export AUTH_TOKEN=$(aws rds generate-db-auth-token --hostname db-test2.c4k4uoc9kxxx.eu-west-3.rds.amazonaws.com --username <master-username> --port 5432)```

NB: because the instance created is based on integration snapshot, the password used to connect is the same as the
integration database (cf Parameter Store in AWS console). So the token is not useful.
