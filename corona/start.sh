#!/bin/bash

# Start the cron service
service cron start

# Ensure that cron is running
if service cron status; then
    echo "Cron service started successfully."
else
    echo "Failed to start cron service."
    exit 1
fi

# Export environment variables to /etc/environment
printenv > /etc/environment

# Find the path to the Python interpreter
PYTHON_PATH=$(which python)

# Set up the cron job to run the Python script at 11 PM everynight
echo "00 23 * * * cd /app && $PYTHON_PATH main.py >> /var/log/cron.log 2>&1" > /etc/cron.d/corona

# Set the correct permissions for the cron file
chmod 0644 /etc/cron.d/corona

# Apply the cron job
crontab /etc/cron.d/corona

# Keep the container running and follow the cron log
tail -f /var/log/cron.log
