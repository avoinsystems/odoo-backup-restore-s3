#!/bin/sh

set -e

if [ "${SCHEDULE}" = "single" ] || [ "$1" = "restore" ]; then
  exec python backup.py "$@"
else
  exec go-cron "$SCHEDULE" python backup.py "$@"
fi
