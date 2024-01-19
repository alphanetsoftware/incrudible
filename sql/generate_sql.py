import yaml

######################################################################################
#### 1. Gets the table specification and gets it ready to generate sql statements ####
######################################################################################

# Parse the YAML file
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Extract the table name, column names, and data types
table_name = config['table']['name']
data_columns = config['table']['columns']

# Specifies the audit columns and data types
audit_columns = [
    {'name': 'id', 'type': 'SERIAL PRIMARY KEY'},
    {'name': 'business_key', 'type': 'UUID'},
    {'name': 'inserted_dttm', 'type': 'TIMESTAMP WITHOUT TIME ZONE'},
    {'name': 'deleted_dttm', 'type': 'TIMESTAMP WITHOUT TIME ZONE'}
]

#Generate the column names and definitions
audit_column_names = ',\n'.join([col['name'] for col in audit_columns])
audit_column_definitions = ',\n'.join([f"{col['name']} {col['type']}" for col in audit_columns])
data_column_names = ',\n'.join([col['name'] for col in data_columns])
data_column_definitions = ',\n'.join([f"{col['name']} {col['type']}" for col in data_columns])
column_names = audit_column_names + ',\n' + data_column_names
column_definitions = audit_column_definitions + ',\n' + data_column_definitions
column_names_without_id = column_names.replace('id,\n', '')


######################################################################################
#### 2. Generates the sql objects and procedures from the specifications given    ####
######################################################################################


# Generate the SQL commands for the basic objects
create_uuid_extension = 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp";' + '\n'
create_table_command = f"CREATE TABLE IF NOT EXISTS {table_name} (\n{column_definitions}\n);"
create_view_command = f"CREATE OR REPLACE VIEW active_{table_name} AS \nSELECT {column_names} FROM {table_name} WHERE deleted_dttm IS NULL\n;"


#Generate the SQL commands for the stored procedures
create_insert_procedure = f"""
CREATE OR REPLACE PROCEDURE insert_{table_name} (\n{data_column_definitions}\n)\nLANGUAGE plpgsql\nAS $$\nBEGIN
INSERT INTO {table_name} (\n{column_names_without_id}
) VALUES (
uuid_generate_v4(),\nnow() AT TIME ZONE 'UTC',\nNULL,
{data_column_names}\n);\nEND;\n$$;
"""

create_update_procedure = f"""
CREATE OR REPLACE PROCEDURE update_{table_name} (\n_business_key UUID,\n{data_column_definitions}\n)
LANGUAGE plpgsql\nAS $$\nBEGIN
IF EXISTS (SELECT 1 FROM {table_name} WHERE business_key = _business_key AND deleted_dttm IS NULL) THEN
UPDATE {table_name}\nSET deleted_dttm = (now() AT TIME ZONE 'UTC') - INTERVAL '1 second'\nWHERE business_key = _business_key\nAND deleted_dttm IS NULL;
INSERT INTO {table_name} (\n{column_names_without_id}
) VALUES (
_business_key,\nnow() AT TIME ZONE 'UTC',\nNULL,\n{data_column_names}
);\nELSE\nRAISE EXCEPTION 'Could not find a record with that business_key';\nEND IF;\nEND;\n$$;
"""

create_delete_procedure = f"""
CREATE OR REPLACE PROCEDURE delete_{table_name} (_business_key UUID)
LANGUAGE plpgsql\nAS $$\nBEGIN
IF EXISTS (SELECT 1 FROM {table_name} WHERE business_key = _business_key AND deleted_dttm IS NULL) THEN
UPDATE {table_name}\nSET deleted_dttm = (now() AT TIME ZONE 'UTC') - INTERVAL '1 second'\nWHERE business_key = _business_key\nAND deleted_dttm IS NULL;
ELSE\nRAISE EXCEPTION 'Could not find a record with that business_key';\nEND IF;\nEND;\n$$;
"""

######################################################################################
#### 3. Writes the sql queries to create_objects.sql and create_procedures.sql    ####
######################################################################################

#Write the create objects SQL commands to a SQL file
with open('sql/create_objects.sql', 'w') as file:
    file.write(create_uuid_extension + '\n\n')
    file.write(create_table_command + '\n\n')
    file.write(create_view_command + '\n\n')

#Write the create procedures SQL commands to a SQL file
with open('sql/create_procedures.sql', 'w') as file:
    file.write(create_insert_procedure + '\n\n')
    file.write(create_update_procedure + '\n\n')
    file.write(create_delete_procedure + '\n\n')

#Writes a file to create the database. Default name is 'incrudible'.
with open('sql/create_database.sql', 'w') as file:
    file.write("CREATE DATABASE incrudible;")
