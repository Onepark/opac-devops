import boto3
from botocore.exceptions import ClientError

from utils.context import delete_context_from_parameter_store, get_or_create_context_from_param_store

rds = boto3.client(service_name="rds")
ssm = boto3.client(service_name="ssm")


def cleanup_on_failure():
    context = get_or_create_context_from_param_store(ssm)

    if context is None:
        print("No context found in Parameter Store — nothing to clean up.")
        exit(1)

    ephemeral_id = context.get("ephemeralRdsInstanceId")

    if ephemeral_id:
        print(f"Deleting ephemeral RDS instance: {ephemeral_id}")
        try:
            rds.delete_db_instance(
                DBInstanceIdentifier=ephemeral_id,
                SkipFinalSnapshot=True,
            )
            print(f"Deletion initiated for {ephemeral_id}.")
        except rds.exceptions.DBInstanceNotFoundFault:
            print(f"Instance {ephemeral_id} not found — already deleted or never created.")
        except ClientError as exc:
            print(f"Error deleting ephemeral instance: {exc}")
    else:
        print("No ephemeralRdsInstanceId in context — instance was never created, skipping.")

    delete_context_from_parameter_store(ssm)

    # Always exit with a non-zero code so Step Functions marks the execution as FAILED.
    exit(1)


if __name__ == "__main__":
    cleanup_on_failure()
