import yaml
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Load the configuration directly from the YAML file
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

try:
    # Connect to the default database
    conn = psycopg2.connect(dbname=config['default_db'], host=config['host'])

    # psycopg2 extensions to interact with PostgreSQL
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    # Cursor to perform database operations
    cur = conn.cursor()

    # SQL query to create a new database
    cur.execute(f"CREATE DATABASE {config['new_db']};")

    # Close communication with the database
    cur.close()
    print(f"Database '{config['new_db']}' created successfully")

except psycopg2.Error as e:
    print(f"An error occurred: {e}")
finally:
    if conn is not None:
        conn.close()

