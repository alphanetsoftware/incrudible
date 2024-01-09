CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS employees (
    id SERIAL PRIMARY KEY,
    business_key UUID,
    inserted_dttm TIMESTAMP WITHOUT TIME ZONE,
    deleted_dttm TIMESTAMP WITHOUT TIME ZONE,
    firstname TEXT,
    lastname TEXT
);

CREATE OR REPLACE VIEW active_employees AS
SELECT
    id,
    business_key,
    inserted_dttm,
    deleted_dttm,
    firstname,
    lastname
FROM employees
WHERE deleted_dttm IS NULL;