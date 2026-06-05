import logging
import os
import boto3

from utils.aws import (
    setup_logging,
    wait_for_deleted_instance,
)


def _get_rds_client():
    return boto3.client("rds", region_name=os.environ.get("AWS_REGION", "eu-west-3"))


def _cleanup_ephemeral(rds_client, ephemeral_id: str):
    try:
        existing = rds_client.describe_db_instances(DBInstanceIdentifier=ephemeral_id)
        status = existing["DBInstances"][0]["DBInstanceStatus"]
        logging.info(f"Ephemeral {ephemeral_id} exists (status={status}) — deleting")
    except rds_client.exceptions.DBInstanceNotFoundFault:
        logging.info(f"Ephemeral {ephemeral_id} not found — nothing to clean up")
        return

    rds_client.delete_db_instance(
        DBInstanceIdentifier=ephemeral_id,
        SkipFinalSnapshot=True,
    )
    wait_for_deleted_instance(rds_client, ephemeral_id)
    logging.info(f"Ephemeral {ephemeral_id} deleted")


def main():
    setup_logging()
    target_id = os.environ["TARGET_RDS_INSTANCE_ID"]
    ephemeral_id = f"ephemeral-transform-{target_id}"

    logging.info("=== Step: Cleanup on Failure ===")
    rds = _get_rds_client()
    _cleanup_ephemeral(rds, ephemeral_id)
    # Always exit with a non-zero code so Step Functions marks the execution as FAILED.
    exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception(f"Fatal error: {e}")
        exit(1)
