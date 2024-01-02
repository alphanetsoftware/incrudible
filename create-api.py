#to do
# - clean up code / move things into functions
# - allow for bulk updating, deleting etc
# - validation. E.g. expect certain inputs for the post and put apis. Reject if you don't get those
# - add an api to return the schema of the table

from flask import Flask, request, jsonify
import psycopg2
import yaml
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import uuid
from datetime import datetime, timedelta
 
## API statements ##
## curl -X POST -H "Content-Type: application/json" -d '{"firstname":"Louis","lastname":"Greg"}' http://localhost:5000/insert
## curl -X GET http://localhost:5000/get
## curl -X PUT -H "Content-Type: application/json" -d '{"firstname":"Louis","lastname":"Gregory","key":"b7d70577-6a1e-46de-a804-471031b0c9fa"}' http://localhost:5000/update
## curl -X PUT -H "Content-Type: application/json" -d '{"key":"b7d70577-6a1e-46de-a804-471031b0c9fa"}' http://localhost:5000/delete

app = Flask(__name__)
app.json.sort_keys = False # Keep the order of the json keys. This is because the order of the columns is important to remember.

# Load the configuration directly from the YAML file
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Table configuration
table_name = config['table']['name']
columns = [column['name'] for column in config['table']['columns']]
audit_columns = ['key', 'inserted_dttm', 'deleted_dttm']

#insert data into the table
@app.route('/insert', methods=['POST'])
def insert_data():
    data = request.json

    # Validate request data
    if not all(column in data for column in columns):
        return jsonify({'error': 'Invalid data'}), 400

    # Generate new UUID for 'key'
    key = str(uuid.uuid4())

    # Get current timestamp in UTC time for 'inserted_dttm'
    inserted_dttm = datetime.utcnow()

    # Insert data into the table
    cursor = conn.cursor()
    insert_query = f"INSERT INTO {table_name} ({', '.join(audit_columns + columns)}) VALUES (%s, %s, NULL, {', '.join(['%s'] * len(columns))})"
    cursor.execute(insert_query, [key, inserted_dttm] + [data[column] for column in columns])
    conn.commit()
    cursor.close()

    return jsonify({'message': 'Data inserted successfully'})

@app.route('/get', methods=['GET'])
def get_data():

    # Select data from the table
    cursor = conn.cursor()
    select_query = f"SELECT {', '.join(audit_columns + columns)} FROM {table_name}"
    cursor.execute(select_query)
    data = cursor.fetchall()
    cursor.close()

    # Convert data to JSON format
    result = []
    for row in data:
        result.append(dict(zip(audit_columns + columns, row)))

    print(result)

    return jsonify(result)

@app.route('/update', methods=['PUT'])
def update_data():
    data = request.json

    key = data['key']

    # Check if 'key' value exists in the table
    cursor = conn.cursor()
    select_query = f"SELECT COUNT(*) FROM {table_name} WHERE key = %s AND deleted_dttm IS NULL"
    cursor.execute(select_query, (key,))
    count = cursor.fetchone()[0]
    cursor.close()

    if count == 0:
        return jsonify({'message': 'Key does not exist'})

    # Update existing records with the same key and deleted_dttm = null
    cursor = conn.cursor()
    update_query = f"UPDATE {table_name} SET deleted_dttm = %s WHERE key = %s AND deleted_dttm IS NULL"
    current_utc_datetime = datetime.utcnow()
    cursor.execute(update_query, (current_utc_datetime - timedelta(seconds=1), key))
    conn.commit()
    cursor.close()

    # Insert a new record with the same key value, insert_dttm = current utc datetime, and deleted_dttm = null
    cursor = conn.cursor()
    insert_query = f"INSERT INTO {table_name} ({', '.join(audit_columns + columns)}) VALUES (%s, %s, NULL, {', '.join(['%s'] * len(columns))})"
    cursor.execute(insert_query, [key, current_utc_datetime] + [data[column] for column in columns])
    conn.commit()
    cursor.close()

    return jsonify({'message': 'Data updated successfully'})


@app.route('/delete', methods=['PUT'])
def delete_data():
    data = request.json

    key = data['key']

    # Check if 'key' value exists in the table
    cursor = conn.cursor()
    select_query = f"SELECT COUNT(*) FROM {table_name} WHERE key = %s AND deleted_dttm IS NULL"
    cursor.execute(select_query, (key,))
    count = cursor.fetchone()[0]
    cursor.close()

    if count == 0:
        return jsonify({'message': 'Key does not exist'})

    # Update existing records with the same key and deleted_dttm = null
    cursor = conn.cursor()
    update_query = f"UPDATE {table_name} SET deleted_dttm = %s WHERE key = %s AND deleted_dttm IS NULL"
    current_utc_datetime = datetime.utcnow()
    cursor.execute(update_query, (current_utc_datetime - timedelta(seconds=1), key))
    conn.commit()
    cursor.close()

    return jsonify({'message': 'Data deleted successfully'})


if __name__ == '__main__':

    # Connect to the default database
    conn = psycopg2.connect(dbname=config['new_db'], host=config['host'])

    # psycopg2 extensions to interact with PostgreSQL
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    app.run()
