#!/bin/bash

# Get the connection name
connection="$1"

# Get the password
password="$2"

# Run the sql statements, passing the password as a parameter
./sql/run_sql_statements.sh create_database.sql "$connection" postgres "$password"
./sql/run_sql_statements.sh create_objects.sql "$connection" incrudible "$password"
./sql/run_sql_statements.sh create_procedures.sql "$connection" incrudible "$password"