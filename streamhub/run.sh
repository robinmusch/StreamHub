#!/usr/bin/with-contenv bashio

set -e

bashio::log.info "Starting StreamHub 2.0.0..."

exec python3 /app/app.py
