from psycopg2 import sql


SANITIZATION_RULES: dict[str, list[str]] = {
    "access_ways": [
        """SET "desc" = left(encode(digest("desc" || %(salt)s, 'sha256'), 'hex'), 16)
           WHERE "type" = 'license_plate'""",
        """SET "desc" = '+33000000000'
           WHERE "type" = 'phone'""",
    ],
    "customers": [
        """SET email = 'anon_' || left(encode(digest(email || %(salt)s, 'sha256'), 'hex'), 12) || '@test.local',
               firstname = left(encode(digest(firstname || %(salt)s, 'sha256'), 'hex'), 16),
               lastname = left(encode(digest(lastname || %(salt)s, 'sha256'), 'hex'), 16),
               phone = '+33000000000'""",
    ],
    "entities": [
        """SET contact_info['email'] = to_jsonb('anon_' || left(encode(digest(contact_info ->> 'email' || %(salt)s, 'sha256'), 'hex'), 12) || '@test.local'),
               contact_info['phone'] = '"+33000000000"'::jsonb
           WHERE contact_info is not null""",
    ],
    "entity_settings": [
        """SET email = 'anon_' || left(encode(digest(email || %(salt)s, 'sha256'), 'hex'), 12) || '@test.local',
               phone = '+33000000000'""",
    ],
    "installation_logs": [
        """SET "desc" = left(encode(digest("desc" || %(salt)s, 'sha256'), 'hex'), 16),
               read_desc = left(encode(digest(read_desc || %(salt)s, 'sha256'), 'hex'), 16)""",
    ],
    "invoices": [
        """SET customer_email = 'anon_' || left(encode(digest(customer_email || %(salt)s, 'sha256'), 'hex'), 12) || '@test.local',
               customer_firstname = left(encode(digest(customer_firstname || %(salt)s, 'sha256'), 'hex'), 16),
               customer_lastname = left(encode(digest(customer_lastname || %(salt)s, 'sha256'), 'hex'), 16),
               pdf_creation_data['customer_email'] = to_jsonb('anon_' || left(encode(digest(pdf_creation_data ->> 'customer_email' || %(salt)s, 'sha256'), 'hex'), 12) || '@test.local'),
               pdf_creation_data['customer_firstname'] = to_jsonb(left(encode(digest(pdf_creation_data ->> 'customer_firstname' || %(salt)s, 'sha256'), 'hex'), 16)),
               pdf_creation_data['customer_lastname'] = to_jsonb(left(encode(digest(pdf_creation_data ->> 'customer_lastname' || %(salt)s, 'sha256'), 'hex'), 16))
           WHERE pdf_creation_data is not null""",
    ],
    "parkings": [
        """SET email = 'anon_' || left(encode(digest(email || %(salt)s, 'sha256'), 'hex'), 12) || '@test.local',
               firstname = left(encode(digest(firstname || %(salt)s, 'sha256'), 'hex'), 16),
               lastname = left(encode(digest(lastname || %(salt)s, 'sha256'), 'hex'), 16)""",
    ],
    "partners": [
        """SET email_for_commission_invoices = 'anon_' || left(encode(digest(email_for_commission_invoices || %(salt)s, 'sha256'), 'hex'), 12) || '@test.local',
               iban = left(encode(digest(iban || %(salt)s, 'sha256'), 'hex'), 16),
               name = left(encode(digest(name || %(salt)s, 'sha256'), 'hex'), 16),
               national_identifier = left(encode(digest(national_identifier || %(salt)s, 'sha256'), 'hex'), 16)""",
    ],
    "users": [
        """SET email = 'anon_' || left(encode(digest(email || %(salt)s, 'sha256'), 'hex'), 12) || '@test.local',
               firstname = left(encode(digest(firstname || %(salt)s, 'sha256'), 'hex'), 16),
               lastname = left(encode(digest(lastname || %(salt)s, 'sha256'), 'hex'), 16)""",
    ],
}


def build_update(table: str, set_clause: str) -> sql.Composed:
    return sql.SQL("UPDATE {} ").format(sql.Identifier(table)) + sql.SQL(set_clause)
