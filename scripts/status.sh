#!/usr/bin/env bash
set -Eeuo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
curl --fail --silent --show-error --max-time 15 "$CONNECT_URL/connectors/$CONNECTOR_NAME/status"
printf '\n'
lab_wait_connector 5 >/dev/null
