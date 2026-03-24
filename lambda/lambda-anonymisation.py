import os
import boto3
from datetime import datetime, timezone
import psycopg2

# this dictionary list all the columns by table where apply anonymisation
anonymisation_table_columns = {
    "access_ways" :
        """SET "desc" = left(md5("desc" || 'SECRET_SALT'), 16)
           WHERE "type" = 'license_plate' OR "type" = 'phone';""" # // Only when type = "license_plate" or "phone"
    ,
    "customers":
        """SET email = 'anon_' || left(md5(email || 'SECRET_SALT'), 12) || '@test.local',
               firstname = left(md5(firstname || 'SECRET_SALT'), 16),
               lastname = left(md5(lastname || 'SECRET_SALT'), 16),
               phone = left(md5(phone || 'SECRET_SALT'), 16);""",
    "entities":
       """SET contact_info['email'] = to_jsonb('anon_' || left(md5(contact_info ->> 'email' || 'SECRET_SALT'), 12) || '@test.local'),
              contact_info['phone'] = to_jsonb(left(md5(contact_info ->> 'phone' || 'SECRET_SALT'), 16))
          WHERE contact_info is not null;""",

    "entity_settings":
        """SET email = 'anon_' || left(md5(email || 'SECRET_SALT'), 12) || '@test.local',
               phone = left(md5(phone || 'SECRET_SALT'), 16);""",
    "installation_logs":
        """SET "desc" = left(md5("desc" || 'SECRET_SALT'), 16),
               read_desc = left(md5(read_desc || 'SECRET_SALT'), 16);""",
    "invoices":
        """SET customer_name = left(md5(customer_name || 'SECRET_SALT'), 16);""" ,
    "parkings":
        """SET email = 'anon_' || left(md5(email || 'SECRET_SALT'), 12) || '@test.local',
               firstname = left(md5(firstname || 'SECRET_SALT'), 16),
               lastname = left(md5(lastname || 'SECRET_SALT'), 16);""",
    "partners":
        """SET email_for_commission_invoices = 'anon_' || left(md5(email_for_commission_invoices || 'SECRET_SALT'), 12) || '@test.local',
               iban = left(md5(iban || 'SECRET_SALT'), 16),
               name = left(md5(name || 'SECRET_SALT'), 16),
               national_identifier = left(md5(national_identifier || 'SECRET_SALT'), 16);""",
    "payments":
        """SET customer_name = left(md5(customer_name || 'SECRET_SALT'), 16);""",
    "users": [
        """SET email = 'anon_' || left(md5(email || 'SECRET_SALT'), 12) || '@test.local',
               firstname = left(md5(firstname || 'SECRET_SALT'), 16),
               lastname = left(md5(lastname || 'SECRET_SALT'), 16);"""
    ]
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
    db_sslrootcerts = os.environ['DB_SSLROOTCERTS']
    db_sslmode = os.environ['DB_SSLMODE']

    try:
        conn = psycopg2.connect(
            host=ephemeral_host,
            port=db_port,
            database=db_name,
            user=db_username,
            password=db_password,
            sslmode=db_sslmode,
            sslrootcert=db_sslrootcerts
        )
        cur = conn.cursor()
        cur.execute('SELECT version();')
        print(cur.fetchone()[0])
        cur.close()
    except Exception as e:
        print(f"Database error: {e}")
        raise

    return conn


def apply_anonymisation(event, context):
    conn = _get_ephemeral_db_connection(event["ephemeral_host"])

    for anon_elements in anonymisation_table_columns.items():
        table = anon_elements[0]
        set_clause = anon_elements[1]

        update_query = f"UPDATE {table} {set_clause}"

        print(f"anonymisation sql query => {update_query}")

        try:
            with conn.cursor() as c:
                c.execute(update_query)
                updated_row_count = c.rowcount
                print(f"Anonymisation updated row count => {updated_row_count} for table {table}")

        except (Exception, psycopg2.DatabaseError) as e:
            print(f"Anonymisation Error on {table} => {set_clause}: {e}")

        conn.commit()

    conn.commit()
    pass


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