#!/usr/bin/env bash
set -Eeuo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
lab_wait_connect
work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
code=$(curl --silent --show-error --max-time 15 --output "$work/response.json" --write-out '%{http_code}' "$CONNECT_URL/connectors/$CONNECTOR_NAME/config")
case "$code" in
  200)
    lab_extract_config "$LAB_ROOT/connectors/bank-mysql.json" > "$work/config.json"
    curl --fail --silent --show-error --max-time 30 -X PUT -H 'Content-Type: application/json' --data-binary "@$work/config.json" "$CONNECT_URL/connectors/$CONNECTOR_NAME/config" > "$work/response.json"
    printf '%s\n' 'Existing connector configuration updated; its stored offsets are preserved.'
    ;;
  404)
    curl --fail --silent --show-error --max-time 30 -X POST -H 'Content-Type: application/json' --data-binary "@$LAB_ROOT/connectors/bank-mysql.json" "$CONNECT_URL/connectors" > "$work/response.json"
    printf '%s\n' 'Connector registered. Initial snapshot begins automatically.'
    ;;
  *)
    printf 'Failed to query connector config (HTTP %s):\n' "$code" >&2
    cat "$work/response.json" >&2
    exit 1
    ;;
esac
lab_wait_connector
