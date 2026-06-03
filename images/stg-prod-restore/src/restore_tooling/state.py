import datetime as dt


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def update_slot_status(
    dynamodb,
    table_name: str,
    slot_name: str,
    updates: dict[str, object],
) -> None:
    if slot_name not in {"blue", "green"}:
        raise RuntimeError(f"Invalid restore slot: {slot_name}")

    expression_parts: list[str] = []
    names: dict[str, str] = {}
    values: dict[str, object] = {}

    for index, (key, value) in enumerate(updates.items()):
        name_token = f"#f{index}"
        value_token = f":v{index}"
        expression_parts.append(f"{name_token} = {value_token}")
        names[name_token] = key
        values[value_token] = value

    if not expression_parts:
        return

    table = dynamodb.Table(table_name)
    table.update_item(
        Key={"pk": f"SLOT#{slot_name}"},
        UpdateExpression="SET " + ", ".join(expression_parts),
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


def mark_running(
    dynamodb,
    table_name: str,
    slot_name: str,
    status_field: str,
    execution_field: str,
    execution_arn: str,
) -> None:
    update_slot_status(
        dynamodb,
        table_name,
        slot_name,
        {
            status_field: "running",
            execution_field: execution_arn,
            f"{status_field}StartedAt": utc_now(),
        },
    )


def mark_passed(dynamodb, table_name: str, slot_name: str, status_field: str) -> None:
    update_slot_status(
        dynamodb,
        table_name,
        slot_name,
        {
            status_field: "passed",
            f"{status_field}CompletedAt": utc_now(),
        },
    )


def mark_failed(
    dynamodb,
    table_name: str,
    slot_name: str,
    status_field: str,
    error_field: str,
    error: str,
) -> None:
    update_slot_status(
        dynamodb,
        table_name,
        slot_name,
        {
            status_field: "failed",
            error_field: error,
            f"{status_field}FailedAt": utc_now(),
        },
    )
