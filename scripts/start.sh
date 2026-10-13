#!/usr/bin/env bash
set -Eeuo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
lab_compose up -d --wait --wait-timeout 240
lab_wait_connect
printf '%s\n' 'Services ready. Adminer: http://localhost:8080 | Kafka UI: http://localhost:8081'
printf '%s\n' 'Next: bash scripts/register-connector.sh, then bash scripts/status.sh.'
