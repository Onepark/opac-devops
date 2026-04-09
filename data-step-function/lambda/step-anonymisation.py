import logging
import os
import boto3
import psycopg2

from utils.context import setup_logging, get_or_create_context_from_param_store
from utils.rds import get_ephemeral_db_connection


anonymisation_table_columns = {
    "access_ways": [
        """SET "desc" = left(encode(digest("desc" || 'SECRET_SALT', 'sha256'), 'hex'), 16)
           WHERE "type" = 'license_plate';""",
        """SET "desc" = '+33000000000'
           WHERE "type" = 'phone';"""
    ],
    "customers": [
        """SET email = 'anon_' || left(encode(digest(email || 'SECRET_SALT', 'sha256'), 'hex'), 12) || '@test.local',
               firstname = left(encode(digest(firstname || 'SECRET_SALT', 'sha256'), 'hex'), 16),
               lastname = left(encode(digest(lastname || 'SECRET_SALT', 'sha256'), 'hex'), 16),
               phone = '+33000000000';"""],
    "entities": [
        """SET contact_info['email'] = to_jsonb('anon_' || left(encode(digest(contact_info ->> 'email' || 'SECRET_SALT', 'sha256'), 'hex'), 12) || '@test.local'),
               contact_info['phone'] = '"+33000000000"'::jsonb
           WHERE contact_info is not null;"""],
    "entity_settings": [
        """SET email = 'anon_' || left(encode(digest(email || 'SECRET_SALT', 'sha256'), 'hex'), 12) || '@test.local',
               phone = '+33000000000';"""],
    "installation_logs": [
        """SET "desc" = left(encode(digest("desc" || 'SECRET_SALT', 'sha256'), 'hex'), 16),
               read_desc = left(encode(digest(read_desc || 'SECRET_SALT', 'sha256'), 'hex'), 16);"""],
    "invoices": [
        """SET customer_email = 'anon_' || left(encode(digest(customer_email || 'SECRET_SALT', 'sha256'), 'hex'), 12) || '@test.local',
               customer_firstname = left(encode(digest(customer_firstname || 'SECRET_SALT', 'sha256'), 'hex'), 16),
               customer_lastname = left(encode(digest(customer_lastname || 'SECRET_SALT', 'sha256'), 'hex'), 16),
               pdf_creation_data['customer_email'] = to_jsonb('anon_' || left(encode(digest(pdf_creation_data ->> 'customer_email' || 'SECRET_SALT', 'sha256'), 'hex'), 12) || '@test.local'),
               pdf_creation_data['customer_firstname'] = to_jsonb(left(encode(digest(pdf_creation_data ->> 'customer_firstname' || 'SECRET_SALT', 'sha256'), 'hex'), 16)),
               pdf_creation_data['customer_lastname'] = to_jsonb(left(encode(digest(pdf_creation_data ->> 'customer_lastname' || 'SECRET_SALT', 'sha256'), 'hex'), 16))
           WHERE pdf_creation_data is not null;"""],
    "parkings": [
        """SET email = 'anon_' || left(encode(digest(email || 'SECRET_SALT', 'sha256'), 'hex'), 12) || '@test.local',
               firstname = left(encode(digest(firstname || 'SECRET_SALT', 'sha256'), 'hex'), 16),
               lastname = left(encode(digest(lastname || 'SECRET_SALT', 'sha256'), 'hex'), 16);"""],
    "partners": [
        """SET email_for_commission_invoices = 'anon_' || left(encode(digest(email_for_commission_invoices || 'SECRET_SALT', 'sha256'), 'hex'), 12) || '@test.local',
               iban = left(encode(digest(iban || 'SECRET_SALT', 'sha256'), 'hex'), 16),
               name = left(encode(digest(name || 'SECRET_SALT', 'sha256'), 'hex'), 16),
               national_identifier = left(encode(digest(national_identifier || 'SECRET_SALT', 'sha256'), 'hex'), 16);"""],
    "users": [
        """SET email = 'anon_' || left(encode(digest(email || 'SECRET_SALT', 'sha256'), 'hex'), 12) || '@test.local',
               firstname = left(encode(digest(firstname || 'SECRET_SALT', 'sha256'), 'hex'), 16),
               lastname = left(encode(digest(lastname || 'SECRET_SALT', 'sha256'), 'hex'), 16);"""]
}

REGION = os.environ['AWS_REGION']
rds = boto3.client(service_name="rds")
ssm = boto3.client(service_name="ssm")


def create_pgcrypto_extension(conn):
    with conn.cursor() as c:
        c.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
    logging.info("pgcrypto extension enabled")


def apply_anonymisation(state_machine_context):
    if not state_machine_context.get("anonymisation", False):
        logging.info("anonymisation=False — skipping anonymisation")
        return

    conn = get_ephemeral_db_connection(rds, state_machine_context)
    create_pgcrypto_extension(conn)

    for table, set_clause_list in anonymisation_table_columns.items():
        for set_clause in set_clause_list:
            update_query = f"UPDATE {table} {set_clause}"
            try:
                with conn.cursor() as c:
                    c.execute(update_query)
                    updated_row_count = c.rowcount
                    logging.info(f"Anonymised {table}: {updated_row_count} rows")
            except (Exception, psycopg2.DatabaseError) as e:
                logging.error(f"Failed to anonymise {table}: {e}")
            conn.commit()

    conn.commit()
    logging.info("Anonymisation complete")


if __name__ == '__main__':
    setup_logging()

    context = get_or_create_context_from_param_store(ssm)

    if context is None:
        logging.error("Context not found in Parameter Store — drifting step may have failed.")
        exit(1)

    logging.info("=== Step: Anonymisation ===")
    apply_anonymisation(state_machine_context=context)
