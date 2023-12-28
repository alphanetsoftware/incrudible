import yaml
import psycopg2

# Load the configuration directly from the YAML file
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

try:
    conn = psycopg2.connect(dbname=config['default_db'], host=config['host'])
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    
    cur = conn.cursor()

    # SQL to drop database
    cur.execute(f"DROP DATABASE IF EXISTS {config['new_db']};")

    print(f"Database '{config['new_db']}' dropped successfully")

except psycopg2.Error as e:
    print(f"An error occurred: {e}")
finally:
    if conn is not None:
        conn.close()
