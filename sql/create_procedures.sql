CREATE OR REPLACE PROCEDURE insert_employee (
    _firstname TEXT,
    _lastname TEXT
    )
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO employees (
        business_key,
        inserted_dttm,
        deleted_dttm,
        firstname,
        lastname
        ) VALUES (
        uuid_generate_v4(),
        now() AT TIME ZONE 'UTC',
        NULL,
        _firstname,
        _lastname
        );
END;
$$;

CREATE OR REPLACE PROCEDURE update_employee (_business_key UUID, _firstname TEXT, _lastname TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM employees WHERE business_key = _business_key AND deleted_dttm IS NULL) THEN
        UPDATE employees 
        SET deleted_dttm = (now() AT TIME ZONE 'UTC') - INTERVAL '1 second'
        WHERE business_key = _business_key
        AND deleted_dttm IS NULL;
        INSERT INTO employees (
            business_key,
            inserted_dttm,
            deleted_dttm,
            firstname,
            lastname
            ) VALUES (
            _business_key,
            now() AT TIME ZONE 'UTC',
            NULL,
            _firstname,
            _lastname
            );
    ELSE
        RAISE EXCEPTION 'Could not find a record with that business_key';
    END IF;
END;
$$;

CREATE OR REPLACE PROCEDURE delete_employee (_business_key UUID)
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM employees WHERE business_key = _business_key AND deleted_dttm IS NULL) THEN
        UPDATE employees 
        SET deleted_dttm = (now() AT TIME ZONE 'UTC') - INTERVAL '1 second'
        WHERE business_key = _business_key
        AND deleted_dttm IS NULL;
    ELSE
        RAISE EXCEPTION 'Could not find a record with that business_key';
    END IF;
END;
$$;