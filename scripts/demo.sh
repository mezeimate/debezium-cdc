#!/usr/bin/env bash
set -Eeuo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
lab_wait_connector >/dev/null
printf '%s\n' 'Watch bank-cdc.bank.PAYMENT in Kafka UI (newest records). Demo key: id=9001.'
lab_sql 'DELETE FROM PAYMENT WHERE id=9001;'
printf '%s\n' 'INSERT PAYMENT 9001: expect op=c.'
lab_sql "INSERT INTO PAYMENT (id,account_id,amount,currency,status,created_at,updated_at) VALUES (9001,1001,12.34,'HUF','NEW',NOW(3),NOW(3));"
sleep 3
printf '%s\n' 'UPDATE PAYMENT 9001: expect op=u, before and after.'
lab_sql "UPDATE PAYMENT SET amount=56.78,status='SETTLED',updated_at=NOW(3) WHERE id=9001;"
sleep 3
printf '%s\n' 'DELETE PAYMENT 9001: expect op=d, after=null, followed by a Kafka tombstone.'
lab_sql 'DELETE FROM PAYMENT WHERE id=9001;'
sleep 3
printf '%s\n' 'Demo finished. A rerun can also produce a cleanup DELETE if a previous run was interrupted.'
