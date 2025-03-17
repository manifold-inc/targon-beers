#!/bin/sh

# Start the cron daemon in the background
crond -b

# Ensure that cron is running
if pgrep crond > /dev/null; then
    echo "Modelo cron service started successfully."
else
    echo "Failed to start cron service."
    exit 1
fi

# Export environment variables to /etc/environment
printenv > /etc/environment

# Create cron.d directory if it doesn't exist
mkdir -p /etc/cron.d

# Set up the cron job to run the Go application at 2:00 AM every night
echo "00 02 * * * cd /app && /app/modelo >> /var/log/cron.log 2>&1" > /etc/cron.d/modelo_cron

# Set the correct permissions for the cron file
chmod 0644 /etc/cron.d/modelo_cron

# Apply the cron job
crontab /etc/cron.d/modelo_cron

# Keep the container running and follow the cron log
tail -f /var/log/cron.log
