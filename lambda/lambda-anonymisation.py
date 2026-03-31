import os
import boto3
import psycopg2

from utils.context import get_or_create_context_from_param_store
from utils.rds import get_ephemeral_db_connection


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

def create_pgcrypto_extension(conn):
    # Enable pgcrypto extension in ephemeral instance
    with conn.cursor() as c:
        c.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")

def apply_anonymisation(state_machine_context):
    if not state_machine_context.get("anonymisation", False):
        print("anonymisation=False => No anonymisation to apply.")
        return

    conn = get_ephemeral_db_connection(rds, state_machine_context)

    create_pgcrypto_extension(conn)

    for anon_elements in anonymisation_table_columns.items():
        table = anon_elements[0]
        set_clause_list = anon_elements[1]

        for set_clause in set_clause_list:
            update_query = f"UPDATE {table} {set_clause}"

            print(f"Anonymisation of table {table} ... ", end='')

            try:
                with conn.cursor() as c:
                    c.execute(update_query)
                    updated_row_count = c.rowcount
                    print(f"OK [{updated_row_count}]")

            except (Exception, psycopg2.DatabaseError) as e:
                print(f"FAILED [Error on {table} => {set_clause}: {e}]")

            conn.commit()

    conn.commit()


if __name__ == '__main__':
    # retrieve the context from previous step function (from parameter store)
    context = get_or_create_context_from_param_store(ssm)

    # Apply anonymisation if requested (anonymisation=True in state_machine_context)
    apply_anonymisation(state_machine_context=context)

    pass