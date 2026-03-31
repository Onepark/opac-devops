import os
import boto3

from utils.context import get_or_create_context_from_param_store, delete_context_from_parameter_store
from utils.rds import wait_for_deleted_instance, wait_for_available_instance

REGION = os.environ['AWS_REGION']
rds = boto3.client(service_name="rds")
ssm = boto3.client(service_name="ssm")

def rename_dance(state_machine_context:dict):
    old_target_instance_id = f"{state_machine_context['targetRdsInstanceId']}-old"

    # - rename target instance to another name
    rds.modify_db_instance(DBInstanceIdentifier=state_machine_context["targetRdsInstanceId"],
                           NewDBInstanceIdentifier=old_target_instance_id,
                           ApplyImmediately=True)

    wait_for_available_instance(rds, db_instance_id=old_target_instance_id)

    # - rename ephemeral to target instance name
    rds.modify_db_instance(DBInstanceIdentifier=state_machine_context["ephemeralRdsInstanceId"],
                           NewDBInstanceIdentifier=state_machine_context["targetRdsInstanceId"],
                           ApplyImmediately=True)

    wait_for_available_instance(rds, db_instance_id=state_machine_context["targetRdsInstanceId"])

    rds.delete_db_instance(DBInstanceIdentifier=old_target_instance_id,
                           SkipFinalSnapshot=True)

    wait_for_deleted_instance(rds, db_instance_id=old_target_instance_id)


if __name__ == '__main__':
    # retrieve the context from previous step function (from parameter store)
    context = get_or_create_context_from_param_store(ssm)

    # Rename dance:
    # - rename target instance to another name
    # - rename ephemeral to target instance name
    # - remove old renamed target instance
    rename_dance(state_machine_context=context)

    # delete context from parameter store (not used anymore as the step function flow has finished)
    delete_context_from_parameter_store(ssm)