#!/usr/bin/with-contenv bashio

set -e

bashio::log.info "Starting StreamHub..."

exec python3 /app/app.py
