DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'incrudible') THEN
        CREATE DATABASE incrudible;
    END IF;
END $$;