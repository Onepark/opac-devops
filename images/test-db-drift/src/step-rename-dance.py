import logging
import os
import boto3

from utils.aws import (
    setup_logging,
    wait_for_available_instance,
    wait_for_deleted_instance,
    wait_for_tcp_port,
)


def _get_rds_client():
    return boto3.client("rds", region_name=os.environ.get("AWS_REGION", "eu-west-3"))


def _check_old_target_conflict(rds_client, old_target_id: str):
    """Check if old target name is already in use."""
    try:
        existing = rds_client.describe_db_instances(DBInstanceIdentifier=old_target_id)
        status = existing["DBInstances"][0]["DBInstanceStatus"]
        if status == "deleting":
            logging.info(f"{old_target_id} is being deleted — waiting before rename")
            wait_for_deleted_instance(rds_client, old_target_id)
            return
        raise RuntimeError(
            f"Cannot rename: {old_target_id} already exists in state '{status}'. "
            f"Manual cleanup required before retry."
        )
    except rds_client.exceptions.DBInstanceNotFoundFault:
        pass


def _verify_connectivity(rds_client, db_instance_id: str, port: int = 5432):
    instance = rds_client.describe_db_instances(DBInstanceIdentifier=db_instance_id)
    endpoint = instance["DBInstances"][0]["Endpoint"]["Address"]
    wait_for_tcp_port(endpoint, port, max_attempts=30, delay=10)
    logging.info(f"Connectivity verified for {db_instance_id} ({endpoint}:{port})")


def _delete_old_target(rds_client, old_target_id: str):
    """Delete the old target RDS and wait for completion. Raises on failure."""
    logging.info(f"Deleting old instance: {old_target_id}")

    # Check current state first
    try:
        existing = rds_client.describe_db_instances(DBInstanceIdentifier=old_target_id)
        status = existing["DBInstances"][0]["DBInstanceStatus"]
    except rds_client.exceptions.DBInstanceNotFoundFault:
        logging.info(f"{old_target_id} already deleted — nothing to do")
        return

    if status == "deleting":
        logging.info(
            f"{old_target_id} is already being deleted — waiting for completion"
        )
        wait_for_deleted_instance(rds_client, old_target_id)
        return

    if status != "available":
        raise RuntimeError(
            f"Cannot delete {old_target_id}: unexpected status '{status}'"
        )

    # Instance is available — initiate deletion
    rds_client.delete_db_instance(
        DBInstanceIdentifier=old_target_id,
        SkipFinalSnapshot=True,
    )
    logging.info(f"Deletion initiated for {old_target_id}")
    wait_for_deleted_instance(rds_client, old_target_id)
    logging.info(f"{old_target_id} deleted successfully")


def rename_dance(rds_client, target_id: str):
    ephemeral_id = f"ephemeral-transform-{target_id}"
    old_target_id = f"{target_id}-old"

    # Step 1: Check no conflict with old target name
    _check_old_target_conflict(rds_client, old_target_id)

    # Step 2: Rename target → old-target
    logging.info(f"Rename dance: {target_id} → {old_target_id}")
    rds_client.modify_db_instance(
        DBInstanceIdentifier=target_id,
        NewDBInstanceIdentifier=old_target_id,
        ApplyImmediately=True,
    )
    wait_for_available_instance(rds_client, old_target_id)
    _verify_connectivity(rds_client, old_target_id)

    # Step 3: Rename ephemeral → target
    logging.info(f"Rename dance: {ephemeral_id} → {target_id}")
    rds_client.modify_db_instance(
        DBInstanceIdentifier=ephemeral_id,
        NewDBInstanceIdentifier=target_id,
        ApplyImmediately=True,
    )
    wait_for_available_instance(rds_client, target_id)
    _verify_connectivity(rds_client, target_id)

    # Step 4: Delete old target
    _delete_old_target(rds_client, old_target_id)


def main():
    setup_logging()
    target_id = os.environ["TARGET_RDS_INSTANCE_ID"]
    execution_arn = os.environ.get("EXECUTION_ARN", "unknown")
    logging.info(
        "=== Step: Rename Dance ===",
        extra={"execution_arn": execution_arn, "target": target_id},
    )

    rds = _get_rds_client()
    rename_dance(rds, target_id)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception(f"Fatal error: {e}")
        exit(1)
