#!/usr/bin/env bash
set -Eeuo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
printf '%s\n' 'This test stops only Connect and changes demo rows 9002/9003. Use it in this shared lab only.'
lab_wait_connector >/dev/null
work=$(mktemp -d)
stopped=0
cleanup() {
  if [[ "$stopped" -eq 1 ]]; then
    printf '%s\n' 'Restoring Connect after an interrupted/failed test...' >&2
    lab_compose start connect || true
  fi
  rm -rf -- "$work"
}
trap cleanup EXIT
lab_sql 'DELETE FROM PAYMENT WHERE id IN (9002,9003);'
sleep 2
seed_offsets=$(lab_payment_offsets)
lab_sql "INSERT INTO PAYMENT (id,account_id,amount,currency,status,created_at,updated_at) VALUES (9002,1001,40.00,'HUF','NEW',NOW(3),NOW(3));"
# offset.flush.interval.ms=1000; allow a checkpoint before stopping Connect.
sleep 3
lab_read_payment_events "$seed_offsets" "$work/seed.jsonl"
if ! lab_has_event "$work/seed.jsonl" 9002 c; then
  printf '%s\n' 'Did not observe streaming INSERT for seed 9002. Finish the initial snapshot and rerun; inspect connector logs if it persists.' >&2
  exit 1
fi
checkpoint=$(lab_payment_offsets)
printf 'Pre-outage Kafka end offsets:\n%s\n' "$checkpoint"
stopped=1
lab_compose stop connect
printf '%s\n' 'Connect stopped. MySQL and Kafka keep running. Writing UPDATE, INSERT, DELETE.'
lab_sql "UPDATE PAYMENT SET amount=42.42,status='RECOVERED',updated_at=NOW(3) WHERE id=9002; INSERT INTO PAYMENT (id,account_id,amount,currency,status,created_at,updated_at) VALUES (9003,1002,3.33,'HUF','NEW',NOW(3),NOW(3)); DELETE FROM PAYMENT WHERE id=9003;"
lab_compose start connect
stopped=0
lab_wait_connector >/dev/null
lab_read_payment_events "$checkpoint" "$work/recovery.jsonl" 15000
printf '%s\n' 'Observed AFTER checkpoint (key TAB event):'
lab_print_recovery_events "$work/recovery.jsonl"
for expectation in '9002 u' '9003 c' '9003 d' '9003 tombstone'; do
  read -r id operation <<< "$expectation"
  if ! lab_has_event "$work/recovery.jsonl" "$id" "$operation"; then
    printf 'Recovery verification failed: missing id=%s op=%s after checkpoint. Inspect Connect logs and the topic.\n' "$id" "$operation" >&2
    exit 1
  fi
done
if lab_has_event "$work/recovery.jsonl" 9002 r || lab_has_event "$work/recovery.jsonl" 9003 r; then
  printf '%s\n' 'Recovery verification failed: a new snapshot event appeared for a recovery row.' >&2
  exit 1
fi
printf '%s\n' 'PASS: outage UPDATE/INSERT/DELETE and tombstone arrived after the checkpoint, with no new snapshot for recovery rows.'
printf '%s\n' 'PAYMENT 9002 remains with status RECOVERED. PAYMENT 9003 was deleted. Reruns reset only those demo rows.'
