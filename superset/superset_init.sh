#!/bin/bash

# create Admin user
superset fab create-admin --username "$ADMIN_USERNAME" --firstname Superset --lastname Admin --email "$ADMIN_EMAIL" --password "$ADMIN_PASSWORD"

# Upgrading Superset metastore
superset db upgrade

# Load Superset Examples
#superset load_examples

# setup roles and permissions
superset superset init

superset import-directory -o -f /app/assets

# Starting server
/bin/sh -c /usr/bin/run-server.sh