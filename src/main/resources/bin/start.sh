#!/bin/bash

# Change to the script's directory
cd "$(dirname "$0")"

APP_NAME="db-data-transfer"
JAR_PATH="../lib/db-data-transfer-1.0.0.jar"
CONFIG_PATH="../config/application.yml"

echo "Starting the application..."

# Start the application with a custom config location.
# Output is redirected to /dev/null to prevent nohup.out.
nohup java -Xms512m -Xmx2g -jar "$JAR_PATH" \
  --spring.config.location=file:"$CONFIG_PATH" > /dev/null 2>&1 &

echo "Application started. Check logs at the configured location."