#!/bin/bash

# Cloud SQL Proxy details
CLOUD_SQL_PROXY_PATH="./cloud-sql-proxy"
INSTANCE_CONNECTION_NAME="$2"

# Get SQL statement to run
SQL=$(cat "$1")

# Database connection parameters
DB_NAME="$3"
DB_USER="postgres"
DB_PASSWORD="$4"
DB_HOST="localhost"
DB_PORT="5432"

# Start the Cloud SQL Proxy and get the PID
$CLOUD_SQL_PROXY_PATH $INSTANCE_CONNECTION_NAME &
CLOUD_SQL_PROXY_PID=$!

# Function to check if the proxy is ready
check_proxy_ready() {
    for i in {1..30}; do
        # Try to connect to the database (modify this command to suit your specific check)
        sleep 5
        PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c '\q' &>/dev/null

        if [ $? -eq 0 ]; then
            echo "Cloud SQL Proxy is ready."
            return 0
        else
            echo "Waiting for Cloud SQL Proxy to be ready..."
        fi
    done

    echo "Cloud SQL Proxy failed to start."
    return 1
}

# Wait for Cloud SQL Proxy to be ready
if ! check_proxy_ready; then
    exit 1
fi

# Then execute the SQL statement
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "$SQL"

#Wait 5 seconds just in case
sleep 5

#Kill the cloud sql proxy so it can be used again
kill $CLOUD_SQL_PROXY_PID