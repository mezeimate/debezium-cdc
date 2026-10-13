#!/usr/bin/env bash
set -Eeuo pipefail
LAB_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
CONNECT_URL='http://localhost:8083'
CONNECTOR_NAME='bank-mysql'
PAYMENT_TOPIC='bank-cdc.bank.PAYMENT'

lab_compose() {
  docker compose --project-directory "$LAB_ROOT" -f "$LAB_ROOT/docker-compose.yml" "$@"
}

lab_sql() {
  lab_compose exec -T -e MYSQL_PWD=root_lab_password mysql mysql -u root bank --execute "$1"
}

lab_wait_connect() {
  local deadline=$((SECONDS + ${1:-120}))
  while (( SECONDS < deadline )); do
    if curl --fail --silent --max-time 10 "$CONNECT_URL/connectors" >/dev/null 2>&1; then return 0; fi
    sleep 2
  done
  printf '%s\n' 'Kafka Connect REST API did not become ready. Check: docker compose logs connect' >&2
  return 1
}

lab_wait_connector() {
  local deadline=$((SECONDS + ${1:-120})) status states nonrunning count
  while (( SECONDS < deadline )); do
    if status=$(curl --fail --silent --max-time 10 "$CONNECT_URL/connectors/$CONNECTOR_NAME/status"); then
      states=$(printf '%s' "$status" | grep -oE '"state"[[:space:]]*:[[:space:]]*"[^"]*"' | sed -E 's/.*:[[:space:]]*"([^"]*)"/\1/' || true)
      if [[ "$states" == *FAILED* ]]; then
        printf 'Connector/task FAILED: %s\n' "$status" >&2
        return 1
      fi
      count=$(printf '%s\n' "$states" | grep -c '^RUNNING$' || true)
      nonrunning=$(printf '%s\n' "$states" | grep -v '^RUNNING$' || true)
      if [[ "$count" -ge 2 && -z "$nonrunning" ]]; then
        printf '%s\n' "$status"
        return 0
      fi
    fi
    sleep 2
  done
  printf '%s\n' 'Connector and tasks did not become RUNNING. Run scripts/status.sh and inspect connect logs.' >&2
  return 1
}

lab_payment_offsets() {
  local raw offsets
  raw=$(lab_compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic "$PAYMENT_TOPIC" --time -1)
  offsets=$(printf '%s\n' "$raw" | awk '/^bank-cdc\.bank\.PAYMENT:[0-9]+:[0-9]+$/')
  if [[ -z "$offsets" ]]; then
    printf '%s\n' 'The PAYMENT topic has no partitions yet. Register the connector and wait for the initial snapshot.' >&2
    return 1
  fi
  printf '%s\n' "$offsets"
}

lab_is_consumer_idle_timeout() {
  local file=$1 classes unexpected error_lines
  grep -q 'org\.apache\.kafka\.common\.errors\.TimeoutException' "$file" || return 1
  grep -qE 'Processed a total of [0-9]+ messages' "$file" || return 1
  classes=$(grep -oE '([A-Za-z_$][A-Za-z0-9_$]*\.)+([A-Za-z_$][A-Za-z0-9_$]*(Exception|Error)|Exception|Error)\b|\b[A-Z][A-Za-z0-9_$]+(Exception|Error)\b' "$file" || true)
  unexpected=$(printf '%s\n' "$classes" | grep -vFx 'org.apache.kafka.common.errors.TimeoutException' || true)
  [[ -z "$unexpected" ]] || return 1
  error_lines=$(grep -Ei '(^|[[:space:]])ERROR([[:space:]]|$)' "$file" | grep -Fv 'ERROR Error processing message, terminating consumer process:' || true)
  [[ -z "$error_lines" ]]
}

lab_read_payment_events() {
  local offsets=$1 output=$2 timeout=${3:-10000} topic partition offset result error_file
  : > "$output"
  while IFS=: read -r topic partition offset; do
    [[ -n "$topic" ]] || continue
    error_file="${output}.stderr-${partition}"
    if lab_compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server kafka:9092 --topic "$PAYMENT_TOPIC" --partition "$partition" --offset "$offset" \
      --timeout-ms "$timeout" --consumer-property enable.auto.commit=false \
      --property print.key=true --property $'key.separator=\t' --property null.literal=null >> "$output" 2> "$error_file"; then
      :
    else
      result=$?
      # Accept exit 1 only when Kafka reports its expected idle timeout.
      if [[ "$result" -ne 1 ]] || ! lab_is_consumer_idle_timeout "$error_file"; then
        cat "$error_file" >&2
        rm -f -- "$error_file"
        return "$result"
      fi
    fi
    rm -f -- "$error_file"
  done <<< "$offsets"
}

lab_has_event() {
  local file=$1 id=$2 operation=$3 line key value
  local id_pattern='"id"[[:space:]]*:[[:space:]]*'"$id"'([^0-9]|$)'
  local op_pattern='"op"[[:space:]]*:[[:space:]]*"'"$operation"'"'
  while IFS= read -r line; do
    [[ "$line" == *$'\t'* ]] || continue
    key=${line%%$'\t'*}
    value=${line#*$'\t'}
    [[ "$key" =~ $id_pattern ]] || continue
    if [[ "$operation" == 'tombstone' ]]; then
      [[ "$value" == 'null' ]] && return 0
      continue
    fi
    [[ "$value" =~ $op_pattern ]] || continue
    if [[ "$operation" == 'u' ]]; then
      [[ "$value" =~ '"before"'[[:space:]]*:[[:space:]]*\{ ]] || continue
      [[ "$value" =~ '"after"'[[:space:]]*:[[:space:]]*\{ ]] || continue
      [[ "$value" =~ '"status"'[[:space:]]*:[[:space:]]*'"RECOVERED"' ]] || continue
    elif [[ "$operation" == 'd' ]]; then
      [[ "$value" =~ '"before"'[[:space:]]*:[[:space:]]*\{ ]] || continue
      [[ "$value" =~ '"after"'[[:space:]]*:[[:space:]]*null ]] || continue
    fi
    return 0
  done < "$file"
  return 1
}

lab_print_recovery_events() {
  local file=$1 line key
  local id_pattern='"id"[[:space:]]*:[[:space:]]*(9002|9003)([^0-9]|$)'
  while IFS= read -r line; do
    key=${line%%$'\t'*}
    if [[ "$key" =~ $id_pattern ]]; then printf '%s\n' "$line"; fi
  done < "$file"
}

lab_extract_config() {
  # Extract the config object without a jq/Python dependency; honor quoted braces.
  awk '
    BEGIN { collecting=0; depth=0; quoted=0; escaped=0; started=0 }
    !collecting && /"config"[[:space:]]*:/ {
      sub(/^.*"config"[[:space:]]*:[[:space:]]*/, ""); collecting=1
    }
    collecting {
      for (i=1; i<=length($0); i++) {
        c=substr($0,i,1)
        if (!started) { if (c!="{") continue; started=1 }
        printf "%s", c
        if (quoted) {
          if (escaped) escaped=0
          else if (c=="\\") escaped=1
          else if (c=="\"") quoted=0
        } else {
          if (c=="\"") quoted=1
          else if (c=="{") depth++
          else if (c=="}") depth--
        }
        if (started && depth==0) { printf "\n"; exit }
      }
      printf "\n"
    }
    END { if (!started || depth!=0 || quoted) exit 1 }
  ' "$1"
}
