# context parameter name for step functions
import json
import os
from botocore.exceptions import ClientError

context_param_name ="/opac/int/step_function/context"

def delete_context_from_parameter_store(ssm):
    try:
        response = ssm.delete_parameter(Name=context_param_name)
        print(f"Successfully deleted: {context_param_name}")
        return response

    except ssm.exceptions.ParameterNotFound:
        print(f"Error: Parameter '{context_param_name}' not found.")
    except ClientError as e:
        print(f"An unexpected error occurred: {e}")


def update_context_in_param_store(ssm, state_machine_context: dict):
    ssm.put_parameter(
        Name=context_param_name,
        Value=json.dumps(state_machine_context),
        Type='SecureString', # secure string because there's password
        Overwrite=True
    )

def dump_context(state_machine_context: dict) -> str:
    # hide password for stdout
    context_dumps = str(state_machine_context).replace(state_machine_context['snapshotDbPassword'], "******")

    return context_dumps

def get_or_create_context_from_param_store(ssm, first: bool = False):
    try:
        state_machine_context_string = ssm.get_parameter(Name=context_param_name, WithDecryption=True)['Parameter']['Value']
        state_machine_context = json.loads(state_machine_context_string)

        print(f"context => {dump_context(state_machine_context)}")

        if first:
            return { "error": "drifting/anonymisation process already in progress" }

        return state_machine_context
    except ssm.exceptions.ParameterNotFound as e:
        if not first:
            print("This parameter /opac/int/step_function/context should exist => return None")
            return None
        else:
            print(f"context not found => create /opac/int/step_function/context !!!")

            execution_name = os.environ.get("EXECUTION_NAME", None)
            state_machine_context_string = os.environ.get("CONTEXT_JSON", None)

            if execution_name and state_machine_context_string:

                state_machine_context = json.loads(state_machine_context_string)

                if "comment" in state_machine_context:
                    del state_machine_context["comment"]

                state_machine_context['executionName'] = execution_name

                ssm.put_parameter(
                    Name=context_param_name,
                    Value=json.dumps(state_machine_context),
                    Type='SecureString',  # secure string because there's password
                    Overwrite=False
                )

                print(f"context => {dump_context(state_machine_context)}")

                return state_machine_context
            else:
                print("EXECUTION_NAME not found => return None")
                return None