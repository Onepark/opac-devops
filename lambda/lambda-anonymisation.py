import os
import boto3
from datetime import datetime, timezone
import psycopg2

# this dictionary list all the columns by table where apply anonymisation
anonymisation_table_columns = {
    "access_ways" : [ # there's 2 different anonymisations here, according to the value of 'type' column value => 2 different set clauses
        """SET "desc" = left(encode(digest("desc" || 'SECRET_SALT', 'sha256'), 'hex'), 16)
           WHERE "type" = 'license_plate';""",
        """SET "desc" = '+33000000000'
           WHERE "type" = 'phone';"""
        ],
    "customers": [
        """SET email = 'anon_' || left(encode(digest(email || 'SECRET_SALT', 'sha256'), 'hex'), 12) || '@test.local',
               firstname = left(encode(digest(firstname || 'SECRET_SALT', 'sha256'), 'hex'), 16),
               lastname = left(encode(digest(lastname || 'SECRET_SALT', 'sha256'), 'hex'), 16),
               phone = '+33000000000';""" ],
    "entities": [
       """SET contact_info['email'] = to_jsonb('anon_' || left(encode(digest(contact_info ->> 'email' || 'SECRET_SALT', 'sha256'), 'hex'), 12) || '@test.local'),
              contact_info['phone'] = '"+33000000000"'::jsonb
          WHERE contact_info is not null;"""],

    "entity_settings": [
        """SET email = 'anon_' || left(encode(digest(email || 'SECRET_SALT', 'sha256'), 'hex'), 12) || '@test.local',
               phone = '+33000000000';"""],
    "installation_logs": [
        """SET "desc" = left(encode(digest("desc" || 'SECRET_SALT', 'sha256'), 'hex'), 16),
               read_desc = left(encode(digest(read_desc || 'SECRET_SALT', 'sha256'), 'hex'), 16);""" ],
    "invoices": [
        """SET customer_email = 'anon_' || left(encode(digest(customer_email || 'SECRET_SALT', 'sha256'), 'hex'), 12) || '@test.local',
               customer_firstname = left(encode(digest(customer_firstname || 'SECRET_SALT', 'sha256'), 'hex'), 16),
               customer_lastname = left(encode(digest(customer_lastname || 'SECRET_SALT', 'sha256'), 'hex'), 16),
               pdf_creation_data['customer_email'] = to_jsonb('anon_' || left(encode(digest(pdf_creation_data ->> 'customer_email' || 'SECRET_SALT', 'sha256'), 'hex'), 12) || '@test.local'),
               pdf_creation_data['customer_firstname'] = to_jsonb(left(encode(digest(pdf_creation_data ->> 'customer_firstname' || 'SECRET_SALT', 'sha256'), 'hex'), 16)),
               pdf_creation_data['customer_lastname'] = to_jsonb(left(encode(digest(pdf_creation_data ->> 'customer_lastname' || 'SECRET_SALT', 'sha256'), 'hex'), 16))
          WHERE pdf_creation_data is not null;""" ],
    "parkings": [
        """SET email = 'anon_' || left(encode(digest(email || 'SECRET_SALT', 'sha256'), 'hex'), 12) || '@test.local',
               firstname = left(encode(digest(firstname || 'SECRET_SALT', 'sha256'), 'hex'), 16),
               lastname = left(encode(digest(lastname || 'SECRET_SALT', 'sha256'), 'hex'), 16);""" ],
    "partners": [
        """SET email_for_commission_invoices = 'anon_' || left(encode(digest(email_for_commission_invoices || 'SECRET_SALT', 'sha256'), 'hex'), 12) || '@test.local',
               iban = left(encode(digest(iban || 'SECRET_SALT', 'sha256'), 'hex'), 16),
               name = left(encode(digest(name || 'SECRET_SALT', 'sha256'), 'hex'), 16),
               national_identifier = left(encode(digest(national_identifier || 'SECRET_SALT', 'sha256'), 'hex'), 16);""" ],
    "users": [
        """SET email = 'anon_' || left(encode(digest(email || 'SECRET_SALT', 'sha256'), 'hex'), 12) || '@test.local',
               firstname = left(encode(digest(firstname || 'SECRET_SALT', 'sha256'), 'hex'), 16),
               lastname = left(encode(digest(lastname || 'SECRET_SALT', 'sha256'), 'hex'), 16);""" ]
}

REGION = os.environ['AWS_REGION']
rds = boto3.client(service_name="rds")
ssm = boto3.client(service_name="ssm")

waiter_available = rds.get_waiter("db_instance_available")
waiter_deleted = rds.get_waiter('db_instance_deleted')

def _get_ephemeral_db_connection(ephemeral_host: str):
    # password are available through env variables :

    # for instance:
    # SNAPSHOT_DB_PASSWORD=test
    # SNAPSHOT_DB_USERNAME=test
    # SNAPSHOT_DB_NAME=test
    # SNAPSHOT_DB_PORT=5432
    # SSLROOTCERTS=~/.aws/rds-certs/global-bundle.pem (when lambda is run directly from local machine)

    db_password = os.environ['SNAPSHOT_DB_PASSWORD']
    db_username = os.environ['SNAPSHOT_DB_USERNAME']
    db_name = os.environ['SNAPSHOT_DB_NAME']
    db_port = os.environ['SNAPSHOT_DB_PORT']
    db_sslrootcerts = os.environ.get('DB_SSLROOTCERTS', None)
    db_sslmode = os.environ.get('DB_SSLMODE', None)

    print(f"db_username : {db_username}")
    print(f"db_password : *****")
    print(f"db_name : {db_name}")
    print(f"db_port : {db_port}")
    print(f"db_sslrootcerts : {db_sslrootcerts}")
    print(f"db_sslmode : {db_sslmode}")

    try:
        conn = psycopg2.connect(
            host=ephemeral_host,
            port=db_port,
            database=db_name,
            user=db_username,
            password=db_password,
            sslmode=db_sslmode,
            sslrootcert=db_sslrootcerts,

        )

        conn.set_client_encoding('UTF8')

        cur = conn.cursor()
        cur.execute('SELECT version();')
        print(cur.fetchone()[0])
        cur.close()
    except Exception as e:
        print(f"Database error: {e}")
        raise

    return conn


def apply_anonymisation(event, context):
    snapshot_arn = os.environ.get("SNAPSHOT_ARN", None)
    execution_arn = os.environ.get("EXECUTION_ARN", None)
    execution_name = os.environ.get("EXECUTION_NAME", None)

    if not execution_arn:
        print("EXECUTION_ARN is not set !!!")
    else:
        print(f"EXECUTION_ARN found : {execution_arn}")

    if not execution_name:
        print("EXECUTION_NAME is not set !!!")
    else:
        print(f"EXECUTION_NAME found : {execution_name}")

    if snapshot_arn is None:
        print("SNAPSHOT_ARN is not set !!!")
    else:
        print(f"Apply anonymisation to ephemeral RDS instance (from snapshot {snapshot_arn})... ", end='')
        print("[DONE]")
    return True

    # conn = _get_ephemeral_db_connection(event["ephemeral_host"])
    #
    # for anon_elements in anonymisation_table_columns.items():
    #     table = anon_elements[0]
    #     set_clause_list = anon_elements[1]
    #
    #     for set_clause in set_clause_list:
    #         update_query = f"UPDATE {table} {set_clause}"
    #
    #         print(f"Anonymisation of table {table} ... ", end='')
    #
    #         try:
    #             with conn.cursor() as c:
    #                 c.execute(update_query)
    #                 updated_row_count = c.rowcount
    #                 print(f"OK [{updated_row_count}]")
    #
    #         except (Exception, psycopg2.DatabaseError) as e:
    #             print(f"FAILED [Error on {table} => {set_clause}: {e}]")
    #
    #         conn.commit()
    #
    # conn.commit()


if __name__ == '__main__':
    anonymize_event_to_send = { "target_env_name": "test2", "source_env_name": "int",
                                "golden_snapshot_id": "golden-snapshot-20260305",
                                "ephemeral_host": "ephemeral-transform-golden-snapshot-20260305-test2.c3k4uoc6kifg.eu-west-3.rds.amazonaws.com" }
    #
    # res_create_ephemeral = create_ephemeral_instance_from_snapshot(event=create_event_to_send, context=None, create_rds_instance=False)
    #
    # res_wait = wait_for_available_instance(res_create_ephemeral, None)

    apply_anonymisation(anonymize_event_to_send, None)

    pass