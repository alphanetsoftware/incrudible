import yaml
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


#This function will construct the sql query to create the new table based on the configuration in config.yaml
def construct_create_table_sql(table_config):
    column_definitions = ', '.join([f"{col['name']} {col['type']}" for col in table_config['columns']])
    return f"CREATE TABLE {table_config['name']} ({column_definitions});"

# Load the configuration directly from the YAML file
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Construct the CREATE TABLE SQL statement
table_config = config['table']
create_table_sql = construct_create_table_sql(table_config)

try:
    # Connect to the default database
    conn = psycopg2.connect(dbname=config['new_db'], host=config['host'])

    # psycopg2 extensions to interact with PostgreSQL
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    # Cursor to perform database operations
    cur = conn.cursor()

    # SQL query to create a new table
    cur.execute(create_table_sql)

    # Close communication with the database
    cur.close()
    print(f"Table '{table_config['name']}' created successfully")

except psycopg2.Error as e:
    print(f"An error occurred: {e}")
finally:
    if conn is not None:
        conn.close()