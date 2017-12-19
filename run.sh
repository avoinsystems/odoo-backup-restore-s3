#!/bin/sh

set -e

if [ "${SCHEDULE}" = "single" ] || [ "$1" = "restore" ]; then
  exec /wait-for-it.sh --strict --timeout=20 -h "$ODOO_HOST" -p "$ODOO_PORT" -- python backup.py "$@"
else
  exec go-cron "$SCHEDULE" python backup.py "$@"
fi
