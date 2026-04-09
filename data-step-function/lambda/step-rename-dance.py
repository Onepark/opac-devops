import logging
import os
import boto3

from utils.context import setup_logging, get_or_create_context_from_param_store, delete_context_from_parameter_store
from utils.rds import wait_for_deleted_instance, wait_for_available_instance

REGION = os.environ['AWS_REGION']
rds = boto3.client(service_name="rds")
ssm = boto3.client(service_name="ssm")


def rename_dance(state_machine_context: dict):
    target_id = state_machine_context['targetRdsInstanceId']
    ephemeral_id = state_machine_context['ephemeralRdsInstanceId']
    old_target_id = f"{target_id}-old"

    logging.info(f"Rename dance: {target_id} → {old_target_id}")
    rds.modify_db_instance(
        DBInstanceIdentifier=target_id,
        NewDBInstanceIdentifier=old_target_id,
        ApplyImmediately=True,
    )
    wait_for_available_instance(rds, db_instance_id=old_target_id)

    logging.info(f"Rename dance: {ephemeral_id} → {target_id}")
    rds.modify_db_instance(
        DBInstanceIdentifier=ephemeral_id,
        NewDBInstanceIdentifier=target_id,
        ApplyImmediately=True,
    )
    wait_for_available_instance(rds, db_instance_id=target_id)

    logging.info(f"Deleting old instance: {old_target_id}")
    rds.delete_db_instance(
        DBInstanceIdentifier=old_target_id,
        SkipFinalSnapshot=True,
    )
    logging.info(f"Deletion initiated for {old_target_id}")

    # wait_for_deleted_instance(rds, db_instance_id=old_target_id)


if __name__ == '__main__':
    setup_logging()

    context = get_or_create_context_from_param_store(ssm)

    if context is None:
        logging.error("Context not found in Parameter Store — drifting step may have failed.")
        exit(1)

    if "ephemeralRdsInstanceId" not in context:
        logging.error("ephemeralRdsInstanceId missing from context — drifting step did not complete successfully.")
        exit(1)

    logging.info("=== Step: Rename Dance ===")
    rename_dance(state_machine_context=context)

    delete_context_from_parameter_store(ssm)
    logging.info("Pipeline complete — SSM context deleted")
