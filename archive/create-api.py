from flask import Flask, request, jsonify
import psycopg2
import yaml
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import uuid
from datetime import datetime, timedelta

# API statements
# curl -X POST -H "Content-Type: application/json" -d '{"firstname":"Louis","lastname":"Gregz23z"}' http://localhost:5000/insert
# curl -X GET http://localhost:5000/get
# curl -X GET http://localhost:5000/schema
# curl -X PUT -H "Content-Type: application/json" -d '{"firstname":"Louis","lastname":"Gregory4","key":"91bb2b52-9a8a-4409-b99a-f7b4ebdfd021"}' http://localhost:5000/update
# curl -X PUT -H "Content-Type: application/json" -d '{"key":"91bb2b52-9a8a-4409-b99a-f7b4ebdfd021"}' http://localhost:5000/delete

app = Flask(__name__)
app.json.sort_keys = False


# Load the configuration directly from the YAML file
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)


# Table configuration
table_name = config['table']['name']
columns = [column['name'] for column in config['table']['columns']]
audit_columns = ['key', 'inserted_dttm', 'deleted_dttm']

# Success and error messages
data_inserted = 'Data inserted successfully'
data_updated = 'Data updated successfully'
data_deleted = 'Data deleted successfully'
invalid_input1 = 'Invalid data, all column values required'
invalid_input2 = 'Invalid data, a key and all column values are required'
invalid_input3 = 'Invalid data, only a key value is required'
invalid_key = 'Key does not exist'


# Executes a query against the database
def execute_query(query, params=None):

    cursor = conn.cursor()
    cursor.execute(query, params)
    conn.commit()
    return cursor


# Fetches all the data from the executed query
def fetch_all(cursor):

    data = cursor.fetchall()
    cursor.close()
    return data


# Checks the database if the key exists
def key_exists(key):

    select_query = (
        f"SELECT COUNT(*) FROM {table_name} " 
        f"WHERE key = %s "
        f"AND deleted_dttm IS NULL"
    )

    cursor = execute_query(select_query, (key,))
    count = fetch_all(cursor)[0][0]
    return count > 0


# Validates the user input includes all columns in the table
def validate_request_data(data, required_columns):

    return set(data.keys()) == set(required_columns)


# API end point to insert data into the table
@app.route('/insert', methods=['POST'])
def insert_data():

    data = request.json

    if not validate_request_data(data, columns):
        return jsonify({'error': invalid_input1}), 400

    # Generate the audit column values
    key = str(uuid.uuid4())
    inserted_dttm = datetime.utcnow()

    # Insert data into the table
    insert_query = (
        f"INSERT INTO {table_name} ({', '.join(audit_columns + columns)}) "
        f"VALUES (%s, %s, NULL, {', '.join(['%s'] * len(columns))})"
    )

    params = [key, inserted_dttm] + [data[column] for column in columns]

    execute_query(insert_query, params)
    return jsonify({'message': data_inserted})


# API end point to return all the data in the table
@app.route('/get', methods=['GET'])
def get_data():

    select_query = (
        f"SELECT {', '.join(audit_columns + columns)} "
        f"FROM {table_name} "
        f"WHERE deleted_dttm IS NULL"
    )

    cursor = execute_query(select_query)
    data = fetch_all(cursor)

    # Convert data to JSON format
    result = []
    for row in data:
        result.append(dict(zip(audit_columns + columns, row)))

    return jsonify(result)


# API end point to update the data using an upsert (update then insert)
@app.route('/update', methods=['PUT'])
def update_data():

    data = request.json

    if not validate_request_data(data, ['key'] + columns):
        return jsonify({'error': invalid_input2}), 400

    key = data['key']

    # Check if 'key' value exists in the table
    if not key_exists(key):
        return jsonify({'message': invalid_key})

    # Update existing records with the same key and deleted_dttm = null
    update_query = (
        f"UPDATE {table_name} SET deleted_dttm = %s "
        f"WHERE key = %s "
        f"AND deleted_dttm IS NULL"
    )

    current_utc_datetime = datetime.utcnow()

    params = (current_utc_datetime - timedelta(seconds=1), key)
    execute_query(update_query, params)

    # Insert a new record with the same key value
    insert_query = (
        f"INSERT INTO {table_name} ({', '.join(audit_columns + columns)}) "
        f"VALUES (%s, %s, NULL, {', '.join(['%s'] * len(columns))})"
    )

    params = [key, current_utc_datetime] + [data[column] for column in columns]

    execute_query(insert_query, params)

    return jsonify({'message': data_updated})


# API end point to delete the data by updating the deleted_dttm column
@app.route('/delete', methods=['PUT'])
def delete_data():
    data = request.json

    # Check if only 'key' is provided in the request data
    if list(data.keys()) != ['key']:
        return jsonify({'error': invalid_input3}), 400

    key = data['key']

    # Check if 'key' value exists in the table
    if not key_exists(key):
        return jsonify({'message': invalid_key})

    # Update existing records with the same key and deleted_dttm = null
    delete_query = (
        f"UPDATE {table_name} SET deleted_dttm = %s "
        f"WHERE key = %s "
        f"AND deleted_dttm IS NULL"
    )

    current_utc_datetime = datetime.utcnow()

    params = (current_utc_datetime - timedelta(seconds=1), key)

    execute_query(delete_query, params)

    return jsonify({'message': data_deleted})


# API end point to return the table schema from the database
@app.route('/schema', methods=['GET'])
def get_schema():
    # Get the table schema

    schema_query = (
        f"SELECT column_name, data_type "
        f"FROM information_schema.columns "
        f"WHERE table_name = %s"
    )

    cursor = execute_query(schema_query, (table_name,))
    schema = fetch_all(cursor)
    return jsonify(schema)


if __name__ == '__main__':

    # Connect to the default database
    conn = psycopg2.connect(dbname=config['new_db'], host=config['host'])

    # psycopg2 extensions to interact with PostgreSQL
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    app.run()
