import json
import logging
import os
import sys
from botocore.exceptions import ClientError

context_param_name = "/opac/int/step_function/context"


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        stream=sys.stdout,
        force=True,
    )


def delete_context_from_parameter_store(ssm):
    try:
        response = ssm.delete_parameter(Name=context_param_name)
        logging.info(f"SSM context deleted: {context_param_name}")
        return response
    except ssm.exceptions.ParameterNotFound:
        logging.warning(
            f"SSM context not found (already deleted?): {context_param_name}"
        )
    except ClientError as e:
        logging.error(f"Unexpected error deleting SSM context: {e}")


def update_context_in_param_store(ssm, state_machine_context: dict):
    ssm.put_parameter(
        Name=context_param_name,
        Value=json.dumps(state_machine_context),
        Type="SecureString",
        Overwrite=True,
    )


def dump_context(state_machine_context: dict) -> str:
    context_dumps = str(state_machine_context).replace(
        state_machine_context["snapshotDbPassword"], "******"
    )
    return context_dumps


def get_or_create_context_from_param_store(ssm, first: bool = False):
    try:
        state_machine_context_string = ssm.get_parameter(
            Name=context_param_name, WithDecryption=True
        )["Parameter"]["Value"]
        state_machine_context = json.loads(state_machine_context_string)

        logging.info(f"Loaded SSM context: {dump_context(state_machine_context)}")

        if first:
            return {"error": "drifting/anonymisation process already in progress"}

        return state_machine_context
    except ssm.exceptions.ParameterNotFound:
        if not first:
            logging.error(
                f"SSM context {context_param_name} not found — previous step may have failed"
            )
            return None
        else:
            logging.info(f"No existing SSM context — creating {context_param_name}")

            execution_name = os.environ.get("EXECUTION_NAME", None)
            state_machine_context_string = os.environ.get("CONTEXT_JSON", None)

            if execution_name and state_machine_context_string:
                state_machine_context = json.loads(state_machine_context_string)

                if "comment" in state_machine_context:
                    del state_machine_context["comment"]

                state_machine_context["executionName"] = execution_name

                ssm.put_parameter(
                    Name=context_param_name,
                    Value=json.dumps(state_machine_context),
                    Type="SecureString",
                    Overwrite=False,
                )

                logging.info(
                    f"SSM context created: {dump_context(state_machine_context)}"
                )
                return state_machine_context
            else:
                logging.error(
                    "EXECUTION_NAME or CONTEXT_JSON env var missing — cannot create SSM context"
                )
                return None
